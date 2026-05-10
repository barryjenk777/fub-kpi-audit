#!/usr/bin/env python3
"""
Legacy Home Team — KPI Audit Dashboard
Flask web app for visualizing agent KPI performance.
"""

import logging
import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("app")

# Load .env if present
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())
from flask import Flask, render_template, jsonify, request

from fub_client import FUBClient
import config
import db as _db
from kpi_audit import (
    auto_detect_agents,
    count_calls_for_user,
    build_excluded_person_ids,
    calculate_speed_to_lead,
    count_compliance_violations,
    count_appointments_for_user,
    evaluate_agent,
)

app = Flask(__name__)

# Initialize Postgres schema (no-op if DATABASE_URL not set)
_db.init_db()

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "settings.json")
CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")

# In-memory settings (used when filesystem is read-only, e.g., Railway)
_memory_settings = {}

# ---- Daily cache: fetch once, serve all day ----
_cache = {}  # In-memory cache: {"audit:2026-03-26": {...}, "manager:2026-03-26": {...}}


def _cache_key(endpoint):
    """Generate a cache key like 'audit:2026-03-26'."""
    return f"{endpoint}:{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"


def cache_get(endpoint):
    """
    Get cached data for today. Returns a shallow copy so callers can safely
    annotate the response (e.g. add from_cache, cache_age) without mutating
    the stored object.

    Tier 1: in-memory (fastest — same worker)
    Tier 2: disk (local dev only)
    Tier 3: Postgres (shared across all gunicorn workers; survives Railway restarts)
    """
    key = _cache_key(endpoint)
    # Tier 1: memory
    if key in _cache:
        return dict(_cache[key])   # shallow copy — callers must not mutate nested lists
    # Tier 2: disk (local dev)
    try:
        path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
                _cache[key] = data  # Promote to memory
                return dict(data)
    except Exception:
        pass
    # Tier 3: Postgres (Railway / multi-worker)
    try:
        data = _db.db_cache_get(endpoint, max_age_hours=24)
        if data:
            _cache[key] = data  # Promote to memory so this worker reuses it
            logger.info("cache_get(%s): served from DB tier (post-restart or cross-worker hit)", endpoint)
            return dict(data)
    except Exception as _dbe:
        logger.debug("cache_get DB tier failed: %s", _dbe)
    return None


def cache_set(endpoint, data):
    """Store data in today's cache. Memory always, disk when possible, DB always."""
    key = _cache_key(endpoint)
    _cache[key] = data
    # Tier 2: disk (local dev)
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
        with open(path, "w") as f:
            json.dump(data, f)
    except OSError:
        pass  # Read-only on Railway
    # Tier 3: Postgres (shared across workers; survives restarts)
    try:
        _db.db_cache_set(endpoint, data)
    except Exception as _dbe:
        logger.debug("cache_set DB tier failed: %s", _dbe)


def cache_clear(endpoint=None):
    """Clear cache for one endpoint or all. Wipes memory, disk, AND Postgres.

    Bug fix: previously only cleared memory + disk. Postgres tier was untouched,
    so cache_warm() would re-read yesterday's Postgres data and store it back
    as today's — perpetually serving stale KPI/ISA/Manager numbers.
    """
    global _cache
    if endpoint:
        key = _cache_key(endpoint)
        _cache.pop(key, None)
        try:
            path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
            os.remove(path)
        except OSError:
            pass
        try:
            _db.db_cache_clear(endpoint)   # wipe Postgres row for this endpoint
        except Exception:
            pass
    else:
        _cache.clear()
        try:
            import shutil
            if os.path.exists(CACHE_DIR):
                shutil.rmtree(CACHE_DIR)
        except OSError:
            pass
        try:
            _db.db_cache_clear()           # wipe entire Postgres api_cache table
        except Exception:
            pass


def load_settings():
    """Load saved KPI thresholds. Priority: Postgres > memory > file > env vars > defaults."""
    # Postgres is the canonical store — survives Railway deploys
    db_settings = _db.load_kpi_settings()
    if db_settings:
        return db_settings
    # Fallback: in-memory (current process only)
    if _memory_settings:
        return dict(_memory_settings)
    # Fallback: settings.json (local dev only — read-only on Railway)
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Fallback: env vars
    s = {}
    if os.environ.get("MIN_CALLS"):
        s["min_calls"] = int(os.environ["MIN_CALLS"])
    if os.environ.get("MIN_CONVOS"):
        s["min_convos"] = int(os.environ["MIN_CONVOS"])
    if os.environ.get("MAX_OOC"):
        s["max_ooc"] = int(os.environ["MAX_OOC"])
    return s


def save_settings(min_calls, min_convos, max_ooc):
    """Persist KPI thresholds. Postgres is primary; memory + file as fallback."""
    global _memory_settings
    data = {"min_calls": min_calls, "min_convos": min_convos, "max_ooc": max_ooc}
    # Postgres — survives deploys
    _db.save_kpi_settings(min_calls, min_convos, max_ooc)
    # Memory — fast reads in current process
    _memory_settings = dict(data)
    # File — local dev only (fails silently on Railway)
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass
    return data


def apply_saved_settings():
    """Apply saved settings to config module."""
    s = load_settings()
    if "min_calls" in s:
        config.MIN_OUTBOUND_CALLS = s["min_calls"]
    if "min_convos" in s:
        config.MIN_CONVERSATIONS = s["min_convos"]
    if "max_ooc" in s:
        config.MAX_OUT_OF_COMPLIANCE = s["max_ooc"]


# Load saved settings on startup
apply_saved_settings()


def _kpi_window(weeks_back=1):
    """Return (since, until) as UTC datetimes for the KPI evaluation window.

    Window: Mon 00:00 ET → Sun 00:00 ET (exclusive) for the most recently
    completed Mon–Sat work week, shifted back (weeks_back-1) additional weeks.

    Uses ET (not UTC) midnight boundaries so:
      - Sunday-evening calls (8pm–midnight ET = midnight–4am UTC Mon) are NOT
        counted toward the next week's Monday tally.
      - Saturday-evening calls (8pm–midnight ET = midnight–4am UTC Sun) ARE
        included in the correct week (they fall before Sun 00:00 ET).

    Example (today ET = Sun Apr 13):
        weeks_back=1 → Mon Apr 7 00:00 ET – Sun Apr 13 00:00 ET
        weeks_back=2 → Mon Mar 31 00:00 ET – Sun Apr 6 00:00 ET
    """
    _et_h = -4 if 3 <= datetime.now(timezone.utc).month <= 10 else -5
    ET = timezone(timedelta(hours=_et_h))
    today_et = datetime.now(ET).replace(hour=0, minute=0, second=0, microsecond=0)
    # How many days ago was last Saturday? (Mon=0 … Sat=5 … Sun=6)
    days_since_sat = (today_et.weekday() - 5) % 7
    if days_since_sat == 0:
        days_since_sat = 7  # today IS Saturday; step back to the previous full week
    # Midnight ET of last Saturday
    last_sat_start = today_et - timedelta(days=days_since_sat)
    # Shift back for weeks_back > 1
    week_offset = timedelta(days=(weeks_back - 1) * 7)
    # until = Sunday 00:00 ET after the target Saturday (exclusive — captures all of Sat ET)
    until_et = last_sat_start + timedelta(days=1) - week_offset
    # since = Monday 00:00 ET of that same work week (6 days before the Sunday boundary)
    since_et = until_et - timedelta(days=6)
    # Return as UTC for FUB API calls
    return since_et.astimezone(timezone.utc), until_et.astimezone(timezone.utc)


def run_audit_data(weeks_back=1, min_calls=None, min_convos=None, max_ooc=None):
    """Run the audit and return structured data for the dashboard."""
    # Apply overrides
    if min_calls is not None:
        config.MIN_OUTBOUND_CALLS = min_calls
    if min_convos is not None:
        config.MIN_CONVERSATIONS = min_convos
    if max_ooc is not None:
        config.MAX_OUT_OF_COMPLIANCE = max_ooc

    client = FUBClient()
    # KPI window: Mon–Sat of the most recently completed work week
    since, until = _kpi_window(weeks_back)

    # Auto-detect agents
    agent_map = auto_detect_agents(client)

    # Try DB cache first (avoids FUB 2000-record cap exhausted by ISA calls).
    # count_calls_for_user() already filters by agent userId so ISA calls in
    # the cache are silently ignored — no change needed downstream.
    cached = _db.get_cached_calls(since=since, until=until)
    if cached:
        all_calls = cached
        print(f"[AUDIT] Using DB cache: {len(all_calls)} calls")
    else:
        # Fall back to live FUB fetch (first run before cache is seeded)
        all_calls = client.get_calls(since=since, until=until)
        print(f"[AUDIT] Live FUB fetch: {len(all_calls)} calls")
    excluded_person_ids = build_excluded_person_ids(client, all_calls)

    # Fetch appointments
    all_appointments = client.get_appointments(since=since, until=until)

    # Collect metrics
    agents = []
    for name, user in sorted(agent_map.items()):
        user_id = user["id"]

        outbound, convos, talk_secs = count_calls_for_user(
            all_calls, user_id, excluded_person_ids
        )
        try:
            speed_avg, speed_count = calculate_speed_to_lead(client, user_id, since)
        except Exception as _stl_err:
            logger.warning("speed_to_lead failed for %s: %s", name, _stl_err)
            speed_avg, speed_count = None, 0
        try:
            violations, ooc_leads, ooc_sphere = count_compliance_violations(
                client, user_id, config.COMPLIANCE_TAG
            )
        except Exception as _ooc_err:
            logger.warning("compliance_violations failed for %s: %s", name, _ooc_err)
            violations, ooc_leads, ooc_sphere = 0, 0, 0
        appts_set, appts_met = count_appointments_for_user(all_appointments, user_id)

        # Text engagement
        try:
            txt_out, txt_in, txt_ppl = client.count_texts_for_user(
                user_id, since=since, until=until, calls=all_calls
            )
        except Exception:
            txt_out = txt_in = txt_ppl = 0

        metrics = {
            "outbound_calls": outbound,
            "conversations": convos,
            "talk_time_seconds": talk_secs,
            "speed_to_lead_avg": speed_avg,
            "speed_to_lead_count": speed_count,
            "compliance_violations": violations,
            "ooc_leads": ooc_leads,
            "ooc_sphere": ooc_sphere,
            "appts_set": appts_set,
            "appts_met": appts_met,
            "texts_out": txt_out,
            "texts_in": txt_in,
            "text_reply_rate": round(txt_in / txt_out * 100) if txt_out else 0,
        }

        evaluation = evaluate_agent(metrics)

        # Conversion rates
        call_to_convo = round(convos / outbound * 100) if outbound > 0 else 0
        convo_to_appt = round(appts_set / convos * 100) if convos > 0 else 0
        set_to_met = round(appts_met / appts_set * 100) if appts_set > 0 else 0

        # Talk time formatted
        talk_h = talk_secs // 3600
        talk_m = (talk_secs % 3600) // 60
        talk_fmt = f"{talk_h}h {talk_m}m" if talk_h > 0 else f"{talk_m}m"

        # Build failure reasons
        failures = []
        if not evaluation["calls_pass"]:
            failures.append(f"Calls {outbound}/{config.MIN_OUTBOUND_CALLS}")
        if not evaluation["convos_pass"]:
            failures.append(f"Convos {convos}/{config.MIN_CONVERSATIONS}")
        if not evaluation["compliance_pass"]:
            failures.append(f"OOC {violations}/{config.MAX_OUT_OF_COMPLIANCE}")
        if not evaluation["speed_pass"] and config.ENABLE_SPEED_TO_LEAD:
            failures.append("STL")

        agents.append({
            "name": name,
            "user_id": user_id,
            "metrics": metrics,
            "evaluation": evaluation,
            "call_to_convo": call_to_convo,
            "convo_to_appt": convo_to_appt,
            "set_to_met": set_to_met,
            "talk_time_fmt": talk_fmt,
            "failures": failures,
        })

    # Rank by score
    for a in agents:
        m = a["metrics"]
        a["score"] = (
            m["outbound_calls"]
            + m["conversations"] * 5
            + m["appts_set"] * 10
            + m["appts_met"] * 20
            - m["compliance_violations"]
        )
    agents.sort(key=lambda x: x["score"], reverse=True)

    # Persist completed-week snapshot so future loads skip FUB API calls
    try:
        _db.save_weekly_kpi_snapshot(
            week_start=since.date(),
            week_end=(until - timedelta(days=1)).date(),
            agents=agents,
        )
    except Exception as _snap_err:
        logger.warning("Weekly snapshot save failed: %s", _snap_err)

    # Team totals
    totals = {
        "calls": sum(a["metrics"]["outbound_calls"] for a in agents),
        "convos": sum(a["metrics"]["conversations"] for a in agents),
        "talk_secs": sum(a["metrics"]["talk_time_seconds"] for a in agents),
        "appts_set": sum(a["metrics"]["appts_set"] for a in agents),
        "appts_met": sum(a["metrics"]["appts_met"] for a in agents),
        "ooc": sum(a["metrics"]["compliance_violations"] for a in agents),
        "passed": sum(1 for a in agents if a["evaluation"]["overall_pass"]),
        "total": len(agents),
    }
    talk_h = totals["talk_secs"] // 3600
    talk_m = (totals["talk_secs"] % 3600) // 60
    totals["talk_fmt"] = f"{talk_h}h {talk_m}m" if talk_h > 0 else f"{talk_m}m"

    # ---- Current week (Mon → now) — "on track?" tracking ----
    now = datetime.now(timezone.utc)
    cw_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cw_dow = cw_today.weekday()  # Mon=0 … Sun=6

    if cw_dow == 6:
        # Sunday — new Mon–Sat work week starts tomorrow; nothing to show yet
        cw_since2 = cw_today + timedelta(days=1)
        cw_until2 = cw_since2
        cw_days_elapsed = 0
        cw_label = f"Starts Mon {(cw_today + timedelta(days=1)).strftime('%b %-d')}"
    else:
        cw_since2 = cw_today - timedelta(days=cw_dow)  # This Monday 00:00
        cw_until2 = now
        cw_days_elapsed = cw_dow + 1  # Mon=1, Tue=2, …, Sat=6
        cw_label = f"Mon {cw_since2.strftime('%b %-d')} – Today"

    try:
        if cw_days_elapsed > 0:
            # Use DB cache (avoids FUB 2000-record cap filled by ISA calls).
            # Fall back to live fetch only if cache is empty (pre-seed).
            cw_calls = _db.get_cached_calls(since=cw_since2, until=cw_until2)
            if not cw_calls:
                cw_calls = client.get_calls(since=cw_since2, until=cw_until2)
            cw_appts_raw = client.get_appointments(since=cw_since2, until=cw_until2)
        else:
            cw_calls = []
            cw_appts_raw = []
    except Exception:
        cw_calls = []
        cw_appts_raw = []

    _min_c = config.MIN_OUTBOUND_CALLS
    _min_v = config.MIN_CONVERSATIONS
    cw_on_pace_count = 0
    cw_agent_data = {}

    for _name, _user in agent_map.items():
        _uid = _user["id"]
        _cw_out, _cw_convos, _ = count_calls_for_user(cw_calls, _uid) if cw_calls else (0, 0, 0)
        _cw_as, _ = count_appointments_for_user(cw_appts_raw, _uid) if cw_appts_raw else (0, 0)

        if cw_days_elapsed > 0:
            _proj_calls = round(_cw_out / cw_days_elapsed * 6)
            _proj_convos = round(_cw_convos / cw_days_elapsed * 6)
        else:
            _proj_calls = _proj_convos = 0

        _on_pace = _proj_calls >= _min_c and _proj_convos >= _min_v
        if _on_pace:
            cw_on_pace_count += 1

        _call_pct = min(round(_cw_out / max(_min_c, 1) * 100), 100)
        _convo_pct = min(round(_cw_convos / max(_min_v, 1) * 100), 100)

        if cw_days_elapsed == 0:
            _pace = "upcoming"
        elif _on_pace:
            _pace = "on_pace"
        elif _proj_calls >= _min_c * 0.7 and _proj_convos >= _min_v * 0.7:
            _pace = "behind"
        else:
            _pace = "at_risk"

        cw_agent_data[_name] = {
            "calls": _cw_out,
            "convos": _cw_convos,
            "appts_set": _cw_as,
            "projected_calls": _proj_calls,
            "projected_convos": _proj_convos,
            "call_pct": _call_pct,
            "convo_pct": _convo_pct,
            "pace_status": _pace,
        }

    routing_week_start = until + timedelta(days=1)  # Monday after the measured Saturday
    routing_week_end = routing_week_start + timedelta(days=6)
    return {
        "agents": agents,
        "totals": totals,
        "period": {
            "start": since.strftime("%b %d"),
            "end": (until - timedelta(days=1)).strftime("%b %d, %Y"),  # Saturday inclusive
            "routing_week": f"{routing_week_start.strftime('%b %d')} – {routing_week_end.strftime('%b %d, %Y')}",
        },
        # Raw datetimes as ISO strings so api_send_email can reconstruct them
        "period_since_iso": since.isoformat(),
        "period_until_iso": until.isoformat(),
        "thresholds": {
            "min_calls": config.MIN_OUTBOUND_CALLS,
            "min_convos": config.MIN_CONVERSATIONS,
            "max_ooc": config.MAX_OUT_OF_COMPLIANCE,
        },
        "current_week": {
            "label": cw_label,
            "days_elapsed": cw_days_elapsed,
            "on_pace_count": cw_on_pace_count,
            "total_agents": len(agent_map),
            "agents": cw_agent_data,
        },
        "live_calls_admin": getattr(config, "LIVE_CALLS_ADMIN", "Admin"),
        "api_requests": client.request_count,
    }


def _build_command_center(audit, manager, deal_summaries, goal_data):
    """Lean mentor briefing — 3-4 sentences, projections, guidance, data 1-liners.

    Team context (baked in — not surfaced to UI):
    - Started 1/1/2026 — building phase, not yet profitable
    - Barry = owner + main trainer + 3 other businesses (limited time)
    - Joe = accountability coach, still learning sales/real estate
    - Fhalen = ISA in Philippines (time zone gap affects call pickup)
    """
    now = datetime.now(timezone.utc)
    year_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    days_in = max((now - year_start).days, 1)
    month_in = round(days_in / 30.4, 1)

    agents  = audit.get("agents", []) if audit else []
    totals  = audit.get("totals", {}) if audit else {}
    period  = audit.get("period", {}) if audit else {}
    thresh  = audit.get("thresholds", {}) if audit else {}

    min_calls  = thresh.get("min_calls", 30)
    min_convos = thresh.get("min_convos", 5)
    n = max(len(agents), 1)

    passed = [a for a in agents if a["evaluation"]["overall_pass"]]
    failed = [a for a in agents if not a["evaluation"]["overall_pass"]]

    calls   = totals.get("calls", 0)
    convos  = totals.get("convos", 0)
    appts   = totals.get("appts_set", 0)
    a_met   = totals.get("appts_met", 0)

    closings  = sum(d.get("closings",  0) for d in deal_summaries.values())
    contracts = sum(d.get("contracts", 0) for d in deal_summaries.values())

    c2v = round(convos / max(calls, 1) * 100)
    v2a = round(appts  / max(convos, 1) * 100)
    annual_pace  = round(closings / days_in * 365) if closings > 0 else 0
    pace_target  = n * 10

    # ── Briefing: 3-4 sentences, tells the story ──────────────────────────────
    kpi_pct = round(len(passed) / n * 100)
    sentences = []

    sentences.append(
        f"Month {month_in} — {calls} calls, {convos} conversations, {appts} appointments, "
        f"{len(passed)}/{n} agents at KPI."
    )

    if calls < n * min_calls * 0.6:
        sentences.append(
            f"Volume is the constraint: {round(calls/n)} calls per agent against a {min_calls}-call bar. "
            f"That's a habit problem — Joe's lane."
        )
    elif c2v < 8 and calls > 30:
        sentences.append(
            f"The dials are there. The conversations aren't — {c2v}% call-to-convo "
            f"against a 10–15% industry rate. Every extra point here is {round(calls * 0.01)} "
            f"more conversations a week without anyone dialing more."
        )
    elif v2a < 15 and convos > 10:
        sentences.append(
            f"Conversations are happening ({convos}), but only {appts} appointments booked — "
            f"{v2a}% convo-to-appt. The ask is missing. One good roleplay session fixes this."
        )
    elif appts > 2 and a_met == 0:
        sentences.append(
            f"{appts} appointments set, zero logged as met. Either they're not happening "
            f"or agents aren't updating FUB — you can't manage what you can't see."
        )

    if closings > 0 or contracts > 0:
        sentences.append(
            f"{closings} closings YTD, {contracts} under contract — proof the model works "
            f"when agents execute."
        )
    elif month_in > 2:
        sentences.append(
            f"No closings yet at month {month_in}. Leads → appointments → contracts is the "
            f"chain to watch — which link breaks first?"
        )

    briefing = " ".join(sentences)

    # ── Guidance: the single highest-leverage move ────────────────────────────
    silent = [a for a in agents if a["metrics"]["outbound_calls"] == 0]
    skill_gap_agents = [
        a for a in agents
        if a["metrics"]["outbound_calls"] >= min_calls * 0.6
        and (round(a["metrics"]["conversations"] / max(a["metrics"]["outbound_calls"], 1) * 100) < 8
             or (a["metrics"]["conversations"] >= min_convos and a["metrics"]["appts_set"] == 0))
    ]

    if silent:
        names = " & ".join(a["name"].split()[0] for a in silent[:2])
        guidance = {
            "action": f"Have Joe call {names} today — not to discipline, to diagnose.",
            "why": f"{'They have' if len(silent)>1 else 'They have'} gone completely quiet. "
                   f"One week of silence at month {month_in} is a yellow flag. "
                   f"Two in a row becomes a pattern the rest of the team notices.",
            "who": "Joe"
        }
    elif skill_gap_agents:
        a = skill_gap_agents[0]
        first = a["name"].split()[0]
        ac2v = round(a["metrics"]["conversations"] / max(a["metrics"]["outbound_calls"], 1) * 100)
        if a["metrics"]["conversations"] >= min_convos and a["metrics"]["appts_set"] == 0:
            guidance = {
                "action": f"20-minute roleplay with {first}: practice the appointment ask.",
                "why": f"{first} is having {a['metrics']['conversations']} conversations "
                       f"and booking zero appointments — the ask isn't there. "
                       f"This is one session away from a different outcome.",
                "who": "Barry"
            }
        else:
            guidance = {
                "action": f"Listen to {first}'s last 5 calls — the problem is in the first 8 seconds.",
                "why": f"{first} is dialing ({a['metrics']['outbound_calls']} calls) "
                       f"but only converting {ac2v}%. The opener isn't landing. "
                       f"This is a Barry coaching moment, not a Joe accountability moment.",
                "who": "Barry"
            }
    elif len(failed) > len(passed):
        guidance = {
            "action": "Answer this before your next team meeting: do they not know what to do, or are they choosing not to do it?",
            "why": f"More agents missing KPIs than hitting them at month {month_in}. "
                   f"The response is completely different depending on the answer — "
                   f"training vs. consequence.",
            "who": "Barry"
        }
    else:
        guidance = {
            "action": "Push the convo-to-appointment rate — that's where your next closings are hiding.",
            "why": f"Activity is solid. At {v2a}% convo-to-appt, moving to 25% means "
                   f"{max(0, round(convos * 0.25) - appts)} more appointments this week "
                   f"without anyone making a single additional call.",
            "who": "Barry"
        }

    # ── Projections: numbers-first, 1 sentence each ───────────────────────────
    projections = []

    if annual_pace > 0:
        gap = pace_target - annual_pace
        projections.append(
            f"~{annual_pace} closings projected this year at YTD pace — "
            f"{'on track' if annual_pace >= pace_target else f'{gap} short of the ~{pace_target}/year needed to sustain a {n}-agent team'}."
        )
    else:
        projections.append(
            f"No closings recorded yet — the clock on year-1 profitability is running. "
            f"A {n}-agent team needs ~{pace_target} closings/year to sustain itself."
        )

    if contracts > 0:
        projections.append(
            f"{contracts} contract{'s' if contracts>1 else ''} in pipeline — "
            f"real revenue 30–45 days out. Protect these closes."
        )

    if c2v > 0:
        extra_convos = max(0, round(calls * 0.12) - convos)
        if extra_convos > 0:
            projections.append(
                f"Closing the call-to-convo gap to 12% (industry avg) = "
                f"+{extra_convos} more conversations/week — no extra dials required."
            )

    if month_in > 3:
        zero_deal_agents = [
            a["name"].split()[0] for a in agents
            if deal_summaries.get(a["name"], {}).get("closings", 0) == 0
            and a["metrics"]["outbound_calls"] < min_calls * 0.5
        ]
        if zero_deal_agents:
            names = ", ".join(zero_deal_agents[:3])
            projections.append(
                f"Month 3–6 is separation time. {names} — low activity + zero closings — "
                f"need a direct 'what's the plan?' conversation in the next 30 days."
            )

    # ── Insights: data 1-liners, industry context ─────────────────────────────
    insights = []

    # Call-to-convo benchmark
    if calls > 20:
        bench_delta = c2v - 12
        if bench_delta < -4:
            insights.append(
                f"Call-to-convo is {c2v}% — {abs(bench_delta)} points below the 10–15% industry rate. "
                f"At {calls} dials, that's {abs(round(calls * bench_delta/100))} missed conversations every week."
            )
        elif bench_delta >= 2:
            insights.append(
                f"Call-to-convo at {c2v}% — above the 10–15% industry benchmark. "
                f"The team is getting people talking. The bottleneck is further downstream."
            )

    # Skill vs effort split
    effort_agents  = [a for a in agents if a["metrics"]["outbound_calls"] >= min_calls * 0.8]
    skill_agents   = [a for a in effort_agents if not a["evaluation"]["overall_pass"]]
    if skill_agents:
        insights.append(
            f"{len(skill_agents)} agent{'s' if len(skill_agents)>1 else ''} making the calls "
            f"but missing KPIs — that's a training problem, not a motivation problem. "
            f"Joe can't fix it. You can."
        )

    # Silent agents
    if silent:
        names = " and ".join(a["name"].split()[0] for a in silent[:2])
        insights.append(
            f"{names} — 0 calls last week. In a building-phase team, silence is contagious. "
            f"The agents who are showing up notice when there's no consequence for those who aren't."
        )

    # Appointment follow-through
    if appts > 0 and a_met == 0:
        insights.append(
            f"{appts} appointments set, zero logged as met. Appointments are your most "
            f"valuable leads — they agreed to talk. Blind spots here mean lost deals."
        )
    elif appts > 0 and a_met > 0:
        met_pct = round(a_met / appts * 100)
        if met_pct < 60:
            insights.append(
                f"{met_pct}% appointment show rate ({a_met}/{appts}). "
                f"Industry average is 70–80% — confirmation calls the day before move this number."
            )

    # ISA time zone note (always relevant, Fhalen is in Philippines)
    if appts > 0:
        insights.append(
            "Fhalen is calling from the Philippines — leads may experience a time zone gap "
            "between her outreach and agent follow-up. Same-business-day handoffs are critical."
        )

    # Joe context
    insights.append(
        f"Joe is your accountability engine, not your sales trainer. "
        f"Effort gaps (calls, show-up) → Joe. Conversion gaps (opener, ask, close) → Barry. "
        f"Mixing these up wastes both of your time."
    )

    return {
        "month_in":    month_in,
        "days_in":     days_in,
        "agent_count": n,
        "period":      period,
        "briefing":    briefing,
        "guidance":    guidance,
        "projections": projections,
        "insights":    insights,
        "stats": {
            "calls":         calls,
            "convos":        convos,
            "appts":         appts,
            "closings_ytd":  closings,
            "contracts":     contracts,
            "annual_pace":   annual_pace,
            "pace_target":   pace_target,
            "c2v":           c2v,
            "v2a":           v2a,
            "passed":        len(passed),
            "total":         n,
        },
    }


@app.route("/api/command-center")
def api_command_center():
    """Barry's mentor briefing — synthesizes all team data into actionable guidance."""
    force = request.args.get("force", "false").lower() == "true"
    try:
        if not force:
            cached = cache_get("command_center")
            if cached:
                cached["from_cache"] = True
                return jsonify(cached)

        # Use cached audit/manager data where possible to avoid double-fetching
        audit = cache_get("audit")
        if not audit:
            try:
                audit = run_audit_data()
                cache_set("audit", audit)
            except Exception as e:
                logger.warning("command-center: audit fetch failed: %s", e)
                audit = {"agents": [], "totals": {}, "period": {}, "thresholds": {}}

        manager = cache_get("manager")  # nice-to-have for trend data; ok if None

        # Deal summaries + goals from DB
        deal_summaries = {}
        goal_data = {}
        for a in (audit.get("agents", []) if audit else []):
            name = a["name"]
            try:
                deal_summaries[name] = _db.get_deal_summary(name, year=datetime.now(timezone.utc).year) or {}
            except Exception:
                deal_summaries[name] = {}
            try:
                g = _db.get_goal(name)
                if g:
                    goal_data[name] = g
            except Exception:
                pass

        result = _build_command_center(audit, manager, deal_summaries, goal_data)
        result["from_cache"] = False
        result["cached_at"] = datetime.now(timezone.utc).isoformat()
        cache_set("command_center", result)
        return jsonify(result)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error("command-center failed: %s\n%s", e, tb)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/audit")
def api_audit():
    min_calls = request.args.get("min_calls", type=int)
    min_convos = request.args.get("min_convos", type=int)
    max_ooc = request.args.get("max_ooc", type=int)
    weeks_back = request.args.get("weeks_back", 1, type=int)
    force = request.args.get("force", "false").lower() == "true"

    try:
        # Check cache (only for default params — custom overrides skip cache)
        if not force and not min_calls and not min_convos and not max_ooc and weeks_back == 1:
            cached = cache_get("audit")
            if cached:
                cached["from_cache"] = True
                # Add human-readable cache age
                cached_at = cached.get("cached_at")
                if cached_at:
                    try:
                        age_mins = int((datetime.now(timezone.utc) - datetime.fromisoformat(cached_at)).total_seconds() / 60)
                        if age_mins < 60:
                            cached["cache_age"] = f"{age_mins}m ago"
                        else:
                            cached["cache_age"] = f"{age_mins // 60}h {age_mins % 60}m ago"
                    except Exception:
                        pass
                # Always re-inject fresh blocks — they change independently of FUB data
                try:
                    _cb = _db.get_all_prospecting_blocks()
                    _cb_map = {b["agent_name"]: b for b in _cb}
                    _cb_lower = {k.lower(): v for k, v in _cb_map.items()}
                    for _ag in cached.get("agents", []):
                        _ag["time_block"] = (
                            _cb_map.get(_ag["name"])
                            or _cb_lower.get(_ag["name"].lower())
                        )
                    cached["prospecting_blocks"] = _cb_map
                except Exception as _cbe:
                    logger.warning("block re-inject on audit cache hit failed: %s", _cbe)
                return jsonify(cached)

        data = run_audit_data(weeks_back, min_calls, min_convos, max_ooc)
        data["from_cache"] = False
        data["cached_at"] = datetime.now(timezone.utc).isoformat()

        # Inject prospecting block data per agent
        try:
            _blocks = _db.get_all_prospecting_blocks()
            _blocks_by_name = {b["agent_name"]: b for b in _blocks}
            _blocks_lower = {k.lower(): v for k, v in _blocks_by_name.items()}
            for agent in data.get("agents", []):
                agent["time_block"] = (
                    _blocks_by_name.get(agent["name"])
                    or _blocks_lower.get(agent["name"].lower())
                )
            data["prospecting_blocks"] = _blocks_by_name
        except Exception as _pb_err:
            logger.warning("prospecting blocks inject (audit) failed: %s", _pb_err)
            data["prospecting_blocks"] = {}

        # Cache default runs only
        if not min_calls and not min_convos and not max_ooc and weeks_back == 1:
            cache_set("audit", data)

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear():
    """Force-clear all caches so next load fetches fresh data."""
    cache_clear()
    return jsonify({"success": True, "message": "Cache cleared — next load will fetch fresh data."})


@app.route("/api/calls-cache/sync", methods=["POST"])
def api_calls_cache_sync():
    """Manually trigger an incremental calls cache sync from FUB.

    Useful on first deploy or after a long downtime to seed/top-up the cache
    before the scheduled every-30-min job fires.
    """
    try:
        fetched = sync_calls_cache()
        return jsonify({"success": True, "fetched": fetched,
                        "message": f"Calls cache sync complete — {fetched} calls fetched from FUB."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    """Get current saved KPI thresholds."""
    return jsonify(load_settings())


@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    """Save KPI thresholds so they persist between runs."""
    data = request.json or {}
    min_calls = data.get("min_calls", config.MIN_OUTBOUND_CALLS)
    min_convos = data.get("min_convos", config.MIN_CONVERSATIONS)
    max_ooc = data.get("max_ooc", config.MAX_OUT_OF_COMPLIANCE)
    saved = save_settings(min_calls, min_convos, max_ooc)
    apply_saved_settings()
    cache_clear()  # KPIs changed — clear stale cache
    return jsonify({"success": True, "settings": saved})


@app.route("/api/update-group", methods=["POST"])
def api_update_group():
    """Update the Priority Agents group in FUB with passing agents."""
    data = request.json or {}
    passed_user_ids = data.get("user_ids", [])

    try:
        client = FUBClient()

        # Add protected agents
        all_ids = list(passed_user_ids)
        if config.PROTECTED_AGENTS:
            for pname in config.PROTECTED_AGENTS:
                puser = client.get_user_by_name(pname)
                if puser and puser["id"] not in all_ids:
                    all_ids.append(puser["id"])

        client.update_group(config.PRIORITY_GROUP_ID, all_ids)
        return jsonify({"success": True, "count": len(all_ids)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send-email", methods=["POST"])
def api_send_email():
    """Send the audit report email."""
    data = request.json or {}

    try:
        from email_report import send_report as _send
        from datetime import datetime, timedelta, timezone

        # Always apply saved settings first (picks up UI changes)
        apply_saved_settings()

        # Re-run audit with provided thresholds (or saved settings as fallback)
        s = load_settings()
        audit_data = run_audit_data(
            weeks_back=1,
            min_calls=data.get("min_calls") or s.get("min_calls"),
            min_convos=data.get("min_convos") or s.get("min_convos"),
            max_ooc=data.get("max_ooc") or s.get("max_ooc"),
        )

        # Use the same Mon–Sat window the audit was built on
        since, until = _kpi_window(1)

        # Build results dict in the format email_report expects
        results = {}
        for a in audit_data["agents"]:
            results[a["name"]] = {
                "user": {"id": a["user_id"]},
                "metrics": a["metrics"],
                "evaluation": a["evaluation"],
            }

        success = _send(results, since, until)
        if success:
            return jsonify({"success": True, "recipients": config.EMAIL_RECIPIENTS})
        else:
            return jsonify({"error": "Email send failed. Check SENDGRID_API_KEY."}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/manager")
def api_manager():
    """Sales Manager tab — deep coaching intelligence for Joe."""
    force = request.args.get("force", "false").lower() == "true"
    try:
        if not force:
            cached = cache_get("manager")
            if cached:
                cached["from_cache"] = True
                cached_at = cached.get("cached_at")
                if cached_at:
                    try:
                        age_mins = int((datetime.now(timezone.utc) - datetime.fromisoformat(cached_at)).total_seconds() / 60)
                        cached["cache_age"] = f"{age_mins // 60}h {age_mins % 60}m ago" if age_mins >= 60 else f"{age_mins}m ago"
                    except Exception:
                        pass
                # Always re-inject fresh prospecting blocks — they change independently
                # of the heavy FUB data pull that populates the rest of the cache.
                try:
                    _cb = _db.get_all_prospecting_blocks()
                    _cb_map = {b["agent_name"]: b for b in _cb}
                    # Case-insensitive fallback map
                    _cb_lower = {k.lower(): v for k, v in _cb_map.items()}
                    for _ag in cached.get("agent_trends", []):
                        _ag["time_block"] = (
                            _cb_map.get(_ag["name"])
                            or _cb_lower.get(_ag["name"].lower())
                        )
                    cached["prospecting_blocks"] = _cb_map
                except Exception as _cbe:
                    logger.warning("block re-inject on cache hit failed: %s", _cbe)
                return jsonify(cached)

        client = FUBClient()
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # Current KPI thresholds (so Joe sees the same bar the agents are held to)
        apply_saved_settings()
        kpi = {
            "min_calls": config.MIN_OUTBOUND_CALLS,
            "min_convos": config.MIN_CONVERSATIONS,
            "max_ooc": config.MAX_OUT_OF_COMPLIANCE,
        }

        # ---- Fetch ALL 4 weeks in ONE batch to save memory/API calls ----
        weeks_data = []
        agent_map = auto_detect_agents(client)

        # Single fetch covering full 4-week range (35d covers worst-case Mon–Sat window)
        full_since = today - timedelta(days=35)

        # Try DB cache first (avoids FUB 2000-record cap exhausted by ISA calls).
        # count_calls_for_user() filters by agent userId so ISA calls are ignored.
        cached_4w = _db.get_cached_calls(since=full_since, until=today)
        if cached_4w:
            all_calls_4w = cached_4w
            print(f"[MANAGER] Using DB cache: {len(all_calls_4w)} calls")
        else:
            all_calls_4w = client.get_calls(since=full_since, until=today)
            print(f"[MANAGER] Live FUB fetch: {len(all_calls_4w)} calls")

        all_appts_4w = client.get_appointments(since=full_since, until=today)

        # Split into Mon–Sat weekly buckets (matches the KPI evaluation window)
        for week_num in range(4):
            since, until = _kpi_window(week_num + 1)
            since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ")

            week_calls = [c for c in all_calls_4w if since_str <= (c.get("created") or "") < until_str]
            week_appts = [a for a in all_appts_4w if since_str <= (a.get("start") or a.get("created") or "") < until_str]

            week_agents = {}
            for name, user in agent_map.items():
                uid = user["id"]
                outbound, convos, talk_secs = count_calls_for_user(week_calls, uid)
                appts_set, appts_met = count_appointments_for_user(week_appts, uid)

                # Text messages for current week only (expensive call)
                texts_out = 0
                texts_in = 0
                if week_num == 0:
                    try:
                        texts_out, texts_in, _ = client.count_texts_for_user(
                            uid, since=since, until=until, calls=all_calls_4w
                        )
                    except Exception:
                        texts_out = texts_in = 0

                # OOC count (current week only)
                ooc_total = 0
                if week_num == 0:
                    try:
                        ooc_total, _, _ = count_compliance_violations(
                            client, uid, config.COMPLIANCE_TAG
                        )
                    except Exception:
                        pass

                week_agents[name] = {
                    "calls": outbound,
                    "convos": convos,
                    "appts_set": appts_set,
                    "appts_met": appts_met,
                    "talk_secs": talk_secs,
                    "texts": texts_out,
                    "texts_in": texts_in,
                    "text_reply_rate": round(texts_in / texts_out * 100) if texts_out else 0,
                    "ooc": ooc_total,
                }

            week_label = f"{since.strftime('%b %d')} – {(until - timedelta(days=1)).strftime('%b %d')}"
            weeks_data.append({"label": week_label, "agents": week_agents})

        # ---- Build per-agent analysis ----
        agent_names = sorted(weeks_data[0]["agents"].keys())
        agent_trends = []

        def trend_dir(curr_val, prev_val):
            if prev_val == 0 and curr_val == 0:
                return "flat"
            if prev_val == 0:
                return "up"
            pct = ((curr_val - prev_val) / prev_val) * 100
            if pct > 15:
                return "up"
            elif pct < -15:
                return "down"
            return "flat"

        def pct_change(curr_val, prev_val):
            if prev_val == 0:
                return 100 if curr_val > 0 else 0
            return round(((curr_val - prev_val) / prev_val) * 100)

        for name in agent_names:
            weeks = [wd["agents"].get(name, {"calls": 0, "convos": 0, "appts_set": 0, "appts_met": 0, "talk_secs": 0, "texts": 0, "ooc": 0}) for wd in weeks_data]
            curr = weeks[0]
            prev = weeks[1] if len(weeks) > 1 else curr

            calls_trend = trend_dir(curr["calls"], prev["calls"])
            convos_trend = trend_dir(curr["convos"], prev["convos"])

            # ---- KPI Pass/Fail for this agent ----
            calls_pass = curr["calls"] >= kpi["min_calls"]
            convos_pass = curr["convos"] >= kpi["min_convos"]
            ooc_pass = curr.get("ooc", 0) <= kpi["max_ooc"]
            kpi_pass = calls_pass and convos_pass and ooc_pass

            # ---- Agent Grade (A-F based on weighted score) ----
            # Calls: 0-100% of target = 0-35 pts
            # Convos: 0-100%+ of target = 0-30 pts
            # Appts: 0-2+ = 0-20 pts
            # OOC penalty: -15 if over threshold
            call_score = min(curr["calls"] / max(kpi["min_calls"], 1), 1.5) * 35
            convo_score = min(curr["convos"] / max(kpi["min_convos"], 1), 2.0) * 30
            appt_score = min(curr["appts_set"] * 10, 20)
            ooc_penalty = -15 if not ooc_pass else 0
            total_score = call_score + convo_score + appt_score + ooc_penalty
            total_score = max(0, min(100, total_score))

            if total_score >= 85:
                grade = "A"
            elif total_score >= 70:
                grade = "B"
            elif total_score >= 55:
                grade = "C"
            elif total_score >= 35:
                grade = "D"
            else:
                grade = "F"

            # ---- Conversion efficiency ----
            call_to_convo = round(curr["convos"] / curr["calls"] * 100) if curr["calls"] > 0 else 0
            convo_to_appt = round(curr["appts_set"] / curr["convos"] * 100) if curr["convos"] > 0 else 0

            # ---- Avg talk time per conversation ----
            avg_talk = round(curr["talk_secs"] / curr["convos"]) if curr["convos"] > 0 else 0
            avg_talk_min = round(avg_talk / 60, 1)

            # ---- Activity consistency (how many of 4 weeks had calls > 0) ----
            active_weeks = sum(1 for w in weeks if w["calls"] > 0)

            # ---- Concern score (higher = needs more attention) ----
            concern = 0
            if curr["calls"] == 0:
                concern += 60
            elif not calls_pass:
                concern += 25
            if calls_trend == "down":
                concern += 20
            if convos_trend == "down":
                concern += 15
            if curr["convos"] == 0 and prev.get("convos", 0) == 0:
                concern += 25
            if not ooc_pass:
                concern += 15
            # Multi-week decline
            if len(weeks) >= 3 and weeks[0]["calls"] < weeks[1]["calls"] < weeks[2]["calls"]:
                concern += 20
            # Low efficiency: making calls but no conversations
            if curr["calls"] >= 20 and curr["convos"] == 0:
                concern += 20
            # Consistent inactivity
            if active_weeks <= 1:
                concern += 15

            # ---- Coaching insights (actionable sentences for Joe) ----
            insights = []
            coaching_type = None  # "accountability" | "skill" | "praise"

            if curr["calls"] == 0:
                insights.append(f"Zero calls this week — schedule an accountability check-in immediately.")
                coaching_type = "accountability"
            elif not calls_pass:
                gap = kpi["min_calls"] - curr["calls"]
                insights.append(f"Only {curr['calls']} calls — {gap} short of the {kpi['min_calls']} minimum. Needs a call block schedule.")
                coaching_type = "accountability"
            elif calls_trend == "down":
                drop = abs(pct_change(curr["calls"], prev["calls"]))
                insights.append(f"Calls dropped {drop}% week-over-week ({prev['calls']} → {curr['calls']}).")
                coaching_type = "accountability"

            if curr["calls"] >= 20 and curr["convos"] == 0:
                insights.append(f"Making {curr['calls']} calls but 0 conversations — may need script coaching or call review.")
                coaching_type = "skill"
            elif curr["calls"] > 0 and call_to_convo < 5 and curr["convos"] > 0:
                insights.append(f"Low call-to-convo rate ({call_to_convo}%) — review call approach and timing.")
                coaching_type = "skill"

            if curr["convos"] > 0 and curr["appts_set"] == 0:
                insights.append(f"Having conversations but not converting to appointments — needs close/ask coaching.")
                coaching_type = "skill"

            if curr["convos"] == 0 and prev.get("convos", 0) == 0:
                insights.append(f"Zero conversations for 2 weeks running.")

            if avg_talk_min > 0 and avg_talk_min < 2.5:
                insights.append(f"Avg convo only {avg_talk_min}m — calls may be ending before building rapport.")

            if not ooc_pass:
                insights.append(f"{curr.get('ooc', 0)} leads out of compliance (max {kpi['max_ooc']}) — clear Maverick nudges.")

            # Positive signals
            if kpi_pass and calls_trend != "down":
                insights.append(f"Meeting all KPIs — acknowledge and challenge to stretch.")
                coaching_type = "praise"
            if curr["appts_set"] > 0 and curr["appts_met"] > 0:
                set_to_met = round(curr["appts_met"] / curr["appts_set"] * 100)
                if set_to_met >= 70:
                    insights.append(f"Strong show rate ({set_to_met}%) — solid follow-through.")

            if not coaching_type:
                coaching_type = "skill" if curr["calls"] > 0 else "accountability"

            agent_trends.append({
                "name": name,
                "current": curr,
                "previous": prev,
                "weeks": weeks,
                "calls_trend": calls_trend,
                "convos_trend": convos_trend,
                "calls_change": pct_change(curr["calls"], prev["calls"]),
                "convos_change": pct_change(curr["convos"], prev["convos"]),
                "grade": grade,
                "score": round(total_score),
                "kpi_pass": kpi_pass,
                "calls_pass": calls_pass,
                "convos_pass": convos_pass,
                "ooc_pass": ooc_pass,
                "call_to_convo": call_to_convo,
                "convo_to_appt": convo_to_appt,
                "avg_talk_min": avg_talk_min,
                "active_weeks": active_weeks,
                "concern": concern,
                "insights": insights,
                "coaching_type": coaching_type,
            })

        # Sort by concern (worst first)
        agent_trends.sort(key=lambda a: a["concern"], reverse=True)

        # ---- Team totals per week ----
        team_weeks = []
        for wd in weeks_data:
            totals = {"calls": 0, "convos": 0, "appts_set": 0, "talk_secs": 0, "texts": 0}
            for ag in wd["agents"].values():
                totals["calls"] += ag["calls"]
                totals["convos"] += ag["convos"]
                totals["appts_set"] += ag["appts_set"]
                totals["talk_secs"] += ag.get("talk_secs", 0)
                totals["texts"] += ag.get("texts", 0)
            team_weeks.append({"label": wd["label"], "totals": totals})

        # ---- Team-wide coaching summary ----
        total_agents = len(agent_trends)
        meeting_kpi = sum(1 for a in agent_trends if a["kpi_pass"])
        need_accountability = [a["name"] for a in agent_trends if a["coaching_type"] == "accountability"]
        need_skill = [a["name"] for a in agent_trends if a["coaching_type"] == "skill"]
        praise_list = [a["name"] for a in agent_trends if a["coaching_type"] == "praise"]

        # Team call-to-convo rate
        tc = team_weeks[0]["totals"]
        team_c2c = round(tc["convos"] / tc["calls"] * 100) if tc["calls"] > 0 else 0

        coaching_summary = {
            "total_agents": total_agents,
            "meeting_kpi": meeting_kpi,
            "pct_meeting": round(meeting_kpi / total_agents * 100) if total_agents > 0 else 0,
            "need_accountability": need_accountability,
            "need_skill": need_skill,
            "praise_list": praise_list,
            "team_call_to_convo": team_c2c,
        }

        # Inject prospecting blocks into each agent trend row
        try:
            _blocks = _db.get_all_prospecting_blocks()
            _blocks_by_name = {b["agent_name"]: b for b in _blocks}
            _blocks_lower = {k.lower(): v for k, v in _blocks_by_name.items()}
            for ag in agent_trends:
                ag["time_block"] = (
                    _blocks_by_name.get(ag["name"])
                    or _blocks_lower.get(ag["name"].lower())
                )
        except Exception as _pb_err:
            logger.warning("prospecting blocks inject (manager) failed: %s", _pb_err)
            _blocks_by_name = {}

        result = {
            "agent_trends": agent_trends,
            "team_weeks": team_weeks,
            "coaching_summary": coaching_summary,
            "kpi": kpi,
            "week_labels": [wd["label"] for wd in weeks_data],
            "from_cache": False,
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "prospecting_blocks": _blocks_by_name,
        }
        cache_set("manager", result)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/send-manager-email", methods=["POST"])
def api_send_manager_email():
    """Send Joe's Monday morning coaching email."""
    try:
        from email_report import send_manager_email

        # Get manager data
        import json as _json
        with app.test_client() as tc:
            resp = tc.get("/api/manager")
            mgr_data = _json.loads(resp.data)

        if "error" in mgr_data:
            return jsonify({"error": mgr_data["error"]}), 500

        period = mgr_data["team_weeks"][0]["label"] if mgr_data["team_weeks"] else "This Week"
        success = send_manager_email(mgr_data, period)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Send failed. Check SENDGRID_API_KEY."}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send-isa-email", methods=["POST"])
def api_send_isa_email():
    """Send Fhalen's ISA performance email."""
    try:
        from email_report import send_isa_email
        import json as _json
        with app.test_client() as tc:
            resp = tc.get("/api/isa")
            isa_data = _json.loads(resp.data)
        if "error" in isa_data:
            return jsonify({"error": isa_data["error"]}), 500
        success = send_isa_email(isa_data)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Send failed. Check SENDGRID_API_KEY."}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/isa")
def api_isa():
    """ISA Performance tab — deep coaching analytics for Fhalen."""
    force = request.args.get("force", "false").lower() == "true"
    try:
        if not force:
            cached = cache_get("isa")
            if cached:
                cached["from_cache"] = True
                cached_at = cached.get("cached_at")
                if cached_at:
                    try:
                        age_mins = int((datetime.now(timezone.utc) - datetime.fromisoformat(cached_at)).total_seconds() / 60)
                        cached["cache_age"] = f"{age_mins // 60}h {age_mins % 60}m ago" if age_mins >= 60 else f"{age_mins}m ago"
                    except Exception:
                        pass
                return jsonify(cached)

        client = FUBClient()
        isa_id = config.ISA_USER_ID
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # Build the full set of FUB user IDs whose calls count as Fhalen's.
        # Fhalen dials through MojoDialer under Barry's account, so those calls
        # land in FUB under Barry's userId and must be merged in.
        isa_user_ids = {isa_id}
        mojo_names = getattr(config, "ISA_MOJO_USER_NAMES", [])
        if mojo_names:
            try:
                all_users = client.get_users()
                name_to_id = {
                    f"{u.get('firstName','')} {u.get('lastName','')}".strip(): u.get("id")
                    for u in all_users
                }
                for n in mojo_names:
                    uid = name_to_id.get(n)
                    if uid:
                        isa_user_ids.add(uid)
                        logger.info("ISA merge: crediting calls from '%s' (uid=%s) to Fhalen", n, uid)
            except Exception as e:
                logger.warning("ISA: could not resolve mojo user IDs: %s", e)

        until_curr = today
        since_curr = today - timedelta(days=7)
        until_prev = since_curr
        since_prev = since_curr - timedelta(days=7)

        # Single batch fetch: 4 weeks of calls + appointments.
        # Try DB cache first (avoids FUB 2000-record cap).
        full_since = today - timedelta(days=28)
        cached_4w = _db.get_cached_calls(since=full_since, until=today)
        if cached_4w:
            all_calls_4w = cached_4w
            print(f"[ISA] Using DB cache: {len(all_calls_4w)} calls")
        else:
            all_calls_4w = client.get_calls(since=full_since, until=today)
            print(f"[ISA] Live FUB fetch: {len(all_calls_4w)} calls")
        all_appts_4w = client.get_appointments(since=full_since, until=today)

        # Filter to Fhalen's calls
        def week_slice(items, s, u, date_key="created"):
            s_str = s.strftime("%Y-%m-%dT%H:%M:%SZ")
            u_str = u.strftime("%Y-%m-%dT%H:%M:%SZ")
            return [i for i in items if s_str <= (i.get(date_key) or "") < u_str]

        calls_curr = week_slice(all_calls_4w, since_curr, until_curr)
        calls_prev = week_slice(all_calls_4w, since_prev, until_prev)

        # ── Call metrics ──
        def isa_calls(all_calls):
            outbound = 0; convos = 0; talk_secs = 0; connected = 0
            for c in all_calls:
                if c.get("userId") not in isa_user_ids:
                    continue
                dur = c.get("duration", 0) or 0
                if not c.get("isIncoming", False):
                    outbound += 1
                    if dur > 0:
                        connected += 1
                if dur >= config.CONVERSATION_THRESHOLD_SECONDS:
                    convos += 1
                    talk_secs += dur
            return outbound, connected, convos, talk_secs

        out_curr, conn_curr, conv_curr, talk_curr = isa_calls(calls_curr)
        out_prev, conn_prev, conv_prev, talk_prev = isa_calls(calls_prev)

        # ── Conversion funnel: dialed → connected → conversation ──
        connect_rate = round(conn_curr / out_curr * 100) if out_curr else 0
        convo_rate = round(conv_curr / conn_curr * 100) if conn_curr else 0
        connect_rate_prev = round(conn_prev / out_prev * 100) if out_prev else 0
        convo_rate_prev = round(conv_prev / conn_prev * 100) if conn_prev else 0

        # ── Call quality: duration buckets ──
        # Include calls from all ISA user IDs (Fhalen's own + MojoDialer under Barry)
        fhalen_out_curr = [c for c in calls_curr if c.get("userId") in isa_user_ids and not c.get("isIncoming")]
        durations = [(c.get("duration", 0) or 0) for c in fhalen_out_curr]
        dur_buckets = {
            "no_answer": sum(1 for d in durations if d == 0),
            "under_30s": sum(1 for d in durations if 0 < d < 30),
            "30s_to_2m": sum(1 for d in durations if 30 <= d < 120),
            "2m_to_5m": sum(1 for d in durations if 120 <= d < 300),
            "over_5m": sum(1 for d in durations if d >= 300),
        }

        # ── Text message engagement ──
        try:
            texts_out_curr, texts_in_curr, texts_people_curr = client.count_texts_for_user(
                isa_id, since=since_curr, until=until_curr, calls=all_calls_4w
            )
            texts_out_prev, texts_in_prev, texts_people_prev = client.count_texts_for_user(
                isa_id, since=since_prev, until=until_prev, calls=all_calls_4w
            )
        except Exception:
            texts_out_curr = texts_in_curr = texts_people_curr = 0
            texts_out_prev = texts_in_prev = texts_people_prev = 0

        text_reply_rate = round(texts_in_curr / texts_out_curr * 100) if texts_out_curr else 0
        text_reply_rate_prev = round(texts_in_prev / texts_out_prev * 100) if texts_out_prev else 0

        # ── Call timing analysis: when does she have the best conversations? ──
        best_hours = {}
        for c in fhalen_out_curr:
            dur = c.get("duration", 0) or 0
            created = c.get("created", "")
            if dur >= 120 and created:
                try:
                    hr = int(created[11:13])
                    # Convert UTC to EST (approximate)
                    hr_est = (hr - 4) % 24
                    slot = f"{hr_est}:00"
                    best_hours[slot] = best_hours.get(slot, 0) + 1
                except (ValueError, IndexError):
                    pass

        # ── Appointments (Fhalen-created calendar events) ──
        def isa_appts(appts_list):
            fhalen_appts = [a for a in appts_list if a.get("createdById") == isa_id]
            met = sum(1 for a in fhalen_appts if a.get("outcome") == "Met with Client")
            no_show = sum(1 for a in fhalen_appts if a.get("outcome") == "No show")
            reschedule = sum(1 for a in fhalen_appts if a.get("outcome") == "Reschedule Needed")
            pending = sum(1 for a in fhalen_appts if a.get("outcome") is None)
            return len(fhalen_appts), met, no_show, reschedule, pending

        appts_curr_list = week_slice(all_appts_4w, since_curr, until_curr, "start")
        appts_prev_list = week_slice(all_appts_4w, since_prev, until_prev, "start")
        appt_set_curr, appt_met_curr, no_show_curr, resched_curr, pending_curr = isa_appts(appts_curr_list)
        appt_set_prev, appt_met_prev, no_show_prev, resched_prev, pending_prev = isa_appts(appts_prev_list)

        show_rate_curr = round(appt_met_curr / appt_set_curr * 100) if appt_set_curr else 0
        show_rate_prev = round(appt_met_prev / appt_set_prev * 100) if appt_set_prev else 0
        calls_per_convo = round(out_curr / conv_curr) if conv_curr else 0

        # ── 4-week sparklines ──
        sparkline_calls = []; sparkline_convos = []; sparkline_appts = []; sparkline_connected = []
        for wk in range(4):
            u = today - timedelta(days=7 * wk)
            s = u - timedelta(days=7)
            wk_calls = week_slice(all_calls_4w, s, u)
            wk_appts = week_slice(all_appts_4w, s, u, "start")
            o, cn, cv, _ = isa_calls(wk_calls)
            a_total, _, _, _, _ = isa_appts(wk_appts)
            label = s.strftime("%b %d")
            sparkline_calls.append({"label": label, "value": o})
            sparkline_connected.append({"label": label, "value": cn})
            sparkline_convos.append({"label": label, "value": cv})
            sparkline_appts.append({"label": label, "value": a_total})
        sparkline_calls.reverse(); sparkline_connected.reverse()
        sparkline_convos.reverse(); sparkline_appts.reverse()

        # ── STALE LEADS: 90-day sweep of all leads Fhalen connected with ──
        # Use 90 days to catch leads from when she was doing recruiting
        lookback_days = getattr(config, "ISA_HANDOFF_LOOKBACK_DAYS", 90)
        # Reuse the 4-week calls if lookback <= 28 days, else fetch more
        if lookback_days <= 28:
            all_calls_sweep = all_calls_4w
        else:
            all_calls_sweep = client.get_calls(since=today - timedelta(days=lookback_days))

        fhalen_connected_sweep = {}
        for c in all_calls_sweep:
            if c.get("userId") in isa_user_ids and (c.get("duration", 0) or 0) > 0:
                pid = c.get("personId")
                if pid and pid not in fhalen_connected_sweep:
                    fhalen_connected_sweep[pid] = c.get("created", "")

        stale_leads = []
        stale_by_agent = {}
        stale_by_stage = {}
        total_handoffs = 0
        active_handoffs = 0

        # Fhalen's own pipeline (leads still assigned to her)
        fhalen_own_pipeline = []
        fhalen_own_stages = {}

        # Cap at 80 people lookups to prevent Railway timeout/crash
        for pid in list(fhalen_connected_sweep.keys())[:80]:
            try:
                person = client._request("GET", f"people/{pid}")
                assigned = person.get("assignedTo", "")
                assigned_uid = person.get("assignedUserId")
                stage = person.get("stage", "Unknown")
                source = person.get("source", "Unknown")
                tags = [t.lower() for t in (person.get("tags") or [])]
                name = person.get("name", "Unknown")
                last_activity = person.get("lastActivity")
                lead_type = person.get("type", "Unknown")

                # Skip excluded lead sources
                if source in config.EXCLUDED_LEAD_SOURCES:
                    continue

                # Calculate days since activity
                days_stale = 999
                if last_activity:
                    try:
                        la_dt = datetime.fromisoformat(last_activity.replace("Z", "+00:00"))
                        days_stale = (today - la_dt).days
                    except (ValueError, TypeError):
                        pass

                # Track Fhalen's own pipeline separately
                if assigned_uid == isa_id:
                    fhalen_own_pipeline.append({
                        "name": name, "person_id": pid, "stage": stage,
                        "source": source, "days_stale": days_stale,
                        "type": lead_type, "first_call": fhalen_connected_sweep.get(pid, "")[:10],
                    })
                    fhalen_own_stages[stage] = fhalen_own_stages.get(stage, 0) + 1
                    continue

                total_handoffs += 1

                if days_stale < config.STALE_LEAD_DAYS:
                    active_handoffs += 1
                    continue

                has_followup_tag = config.ISA_FOLLOWUP_TAG.lower() in tags

                stale_leads.append({
                    "name": name, "person_id": pid, "stage": stage,
                    "assigned_to": assigned, "days_stale": days_stale,
                    "source": source, "has_tag": has_followup_tag,
                    "type": lead_type,
                })

                stale_by_agent[assigned] = stale_by_agent.get(assigned, 0) + 1
                stale_by_stage[stage] = stale_by_stage.get(stage, 0) + 1
            except Exception:
                continue

        # Sort stale leads: most stale first
        stale_leads.sort(key=lambda x: x["days_stale"], reverse=True)

        # Sort Fhalen's own pipeline: most stale first
        fhalen_own_pipeline.sort(key=lambda x: x["days_stale"], reverse=True)
        fhalen_stuck_in_lead = sum(1 for p in fhalen_own_pipeline if p["stage"] == "Lead")
        fhalen_own_stale = [p for p in fhalen_own_pipeline if p["days_stale"] >= 14]

        handoff_success_rate = round(active_handoffs / total_handoffs * 100) if total_handoffs else 0

        # ── COACHING INSIGHTS ──
        insights = []

        # Insight 1: Conversation rate
        if conv_curr == 0:
            insights.append({
                "type": "critical", "icon": "🚨",
                "title": "Zero Conversations This Week",
                "detail": f"Fhalen made {out_curr} calls but had 0 conversations over 2 minutes. "
                          f"Are calls being cut short? Is she reaching voicemail? "
                          f"{dur_buckets['no_answer']} calls got no answer, {dur_buckets['under_30s']} were under 30 seconds.",
            })
        elif conv_curr < 5:
            insights.append({
                "type": "warning", "icon": "⚠️",
                "title": f"Low Conversation Volume ({conv_curr} this week)",
                "detail": f"Out of {conn_curr} connected calls, only {conv_curr} became real conversations. "
                          f"That's a {convo_rate}% conversion. Target: 10%+. "
                          f"Coach on keeping leads engaged past the 2-minute mark.",
            })

        # Insight 2: Stale handoffs
        if len(stale_leads) > 5:
            worst_agent = max(stale_by_agent.items(), key=lambda x: x[1]) if stale_by_agent else ("Nobody", 0)
            insights.append({
                "type": "critical", "icon": "🔴",
                "title": f"{len(stale_leads)} Leads Going Cold After Handoff",
                "detail": f"Fhalen connected with these leads but they've had no activity in {config.STALE_LEAD_DAYS}+ days. "
                          f"{worst_agent[0]} has {worst_agent[1]} stale leads. "
                          f"Fhalen should be re-engaging these — they were warm when she found them. "
                          f"Tag them with ISA_Followup so she knows to circle back.",
            })

        # Insight 3: Appointment outcomes
        total_appts_30d = sum(1 for a in all_appts_4w if a.get("createdById") == isa_id)
        met_30d = sum(1 for a in all_appts_4w if a.get("createdById") == isa_id and a.get("outcome") == "Met with Client")
        no_show_30d = sum(1 for a in all_appts_4w if a.get("createdById") == isa_id and a.get("outcome") == "No show")
        pending_30d = sum(1 for a in all_appts_4w if a.get("createdById") == isa_id and a.get("outcome") is None)

        if total_appts_30d > 0 and pending_30d > total_appts_30d * 0.5:
            insights.append({
                "type": "warning", "icon": "📋",
                "title": f"{pending_30d} of {total_appts_30d} Appointments Have No Outcome Logged",
                "detail": f"Over the past 4 weeks, {round(pending_30d/total_appts_30d*100)}% of Fhalen's appointments "
                          f"have no outcome recorded. Were they met? No-showed? Rescheduled? "
                          f"Without this data, we can't measure her true conversion rate.",
            })

        if no_show_30d > 2:
            insights.append({
                "type": "warning", "icon": "👻",
                "title": f"{no_show_30d} No-Shows in 4 Weeks",
                "detail": f"Are confirmation texts/calls going out before appointments? "
                          f"No-shows waste agent time and tank morale. "
                          f"Implement a same-day confirmation workflow.",
            })

        # Insight 4: Call volume trend
        if len(sparkline_calls) >= 2:
            curr_vol = sparkline_calls[-1]["value"]
            prev_vol = sparkline_calls[-2]["value"]
            if prev_vol > 0 and curr_vol < prev_vol * 0.7:
                insights.append({
                    "type": "warning", "icon": "📉",
                    "title": "Call Volume Dropping",
                    "detail": f"Calls dropped from {prev_vol} to {curr_vol} ({round((1 - curr_vol/prev_vol)*100)}% decline). "
                              f"Is Fhalen spending too much time on admin? Check if dial time is being protected.",
                })

        # Insight 5: ROI check
        if conv_curr <= 2 and out_curr > 200:
            insights.append({
                "type": "info", "icon": "💰",
                "title": "ROI Check: High Activity, Low Output",
                "detail": f"{out_curr} calls for {conv_curr} conversations = {calls_per_convo} calls per conversation. "
                          f"Industry benchmark is 50-80 calls per meaningful conversation. "
                          f"{'This is above benchmark — review call list quality and timing.' if calls_per_convo > 80 else 'Within range but volume needs to increase.'}",
            })

        # Insight 6: Best call times
        if best_hours:
            top_hour = max(best_hours.items(), key=lambda x: x[1])
            insights.append({
                "type": "info", "icon": "🕐",
                "title": f"Best Conversation Time: {top_hour[0]} EST",
                "detail": f"Most conversations happened around {top_hour[0]} EST ({top_hour[1]} convos). "
                          f"Protect this time slot for high-priority dials.",
            })

        # Insight 7: Fhalen's own pipeline health
        if fhalen_stuck_in_lead > 10:
            insights.append({
                "type": "critical", "icon": "🔴",
                "title": f"{fhalen_stuck_in_lead} of {len(fhalen_own_pipeline)} Leads Still in 'Lead' Stage",
                "detail": f"Fhalen connected with {len(fhalen_own_pipeline)} people but {fhalen_stuck_in_lead} "
                          f"haven't progressed past 'Lead' stage. These need to either be qualified and "
                          f"handed off to an agent, or moved to a nurture sequence. Leads sitting in 'Lead' "
                          f"stage after a conversation are wasted opportunities.",
            })

        # Insight 8: Low handoff volume
        if total_handoffs < 5 and len(fhalen_own_pipeline) > 20:
            insights.append({
                "type": "warning", "icon": "🚧",
                "title": f"Only {total_handoffs} Leads Handed Off to Agents (90-day sweep)",
                "detail": f"Fhalen is sitting on {len(fhalen_own_pipeline)} leads in her own pipeline but "
                          f"has only passed {total_handoffs} to agents. The funnel is clogged at the ISA level. "
                          f"She needs to qualify and hand off faster — every day a warm lead sits, it cools down.",
            })

        # Insight 9: Text engagement effectiveness
        if texts_out_curr > 0 and text_reply_rate < 5:
            insights.append({
                "type": "warning", "icon": "💬",
                "title": f"Low Text Reply Rate ({text_reply_rate}%)",
                "detail": f"Fhalen sent {texts_out_curr} texts but only {texts_in_curr} got replies. "
                          f"Review message templates — are they personalized? Do they ask a question? "
                          f"Generic blasts get ignored. Best practice: reference the lead's search criteria.",
            })
        elif texts_out_curr == 0:
            insights.append({
                "type": "warning", "icon": "📱",
                "title": "Zero Texts Sent This Week",
                "detail": "Texting is the #1 way to get a response from online leads. "
                          "Industry data shows text gets 4x the response rate of calls alone. "
                          "Fhalen should be texting every lead she calls — it doubles the contact rate.",
            })

        # Insight 10: Fhalen's stale owned leads
        if len(fhalen_own_stale) > 5:
            insights.append({
                "type": "warning", "icon": "⏰",
                "title": f"{len(fhalen_own_stale)} of Fhalen's Own Leads Are 14+ Days Stale",
                "detail": f"These are leads Fhalen connected with but never moved forward. "
                          f"After 14 days without activity, the lead has likely gone cold. "
                          f"Either re-engage with a new approach or move to long-term nurture.",
            })

        # (duplicate insights removed — already covered above)

        result = {
            "current": {
                "calls": out_curr, "connected": conn_curr, "convos": conv_curr,
                "talk_secs": talk_curr, "appts_set": appt_set_curr,
                "appts_met": appt_met_curr, "no_show": no_show_curr,
                "reschedule": resched_curr, "pending": pending_curr,
                "show_rate": show_rate_curr, "calls_per_convo": calls_per_convo,
                "connect_rate": connect_rate, "convo_rate": convo_rate,
                "texts_out": texts_out_curr, "texts_in": texts_in_curr,
                "texts_people": texts_people_curr, "text_reply_rate": text_reply_rate,
            },
            "previous": {
                "calls": out_prev, "connected": conn_prev, "convos": conv_prev,
                "talk_secs": talk_prev, "appts_set": appt_set_prev,
                "appts_met": appt_met_prev, "show_rate": show_rate_prev,
                "connect_rate": connect_rate_prev, "convo_rate": convo_rate_prev,
                "texts_out": texts_out_prev, "texts_in": texts_in_prev,
                "text_reply_rate": text_reply_rate_prev,
            },
            "duration_buckets": dur_buckets,
            "best_hours": [{"hour": h, "count": c} for h, c in sorted(best_hours.items(), key=lambda x: x[1], reverse=True)[:5]],
            "sparkline": {
                "calls": sparkline_calls, "connected": sparkline_connected,
                "convos": sparkline_convos, "appts": sparkline_appts,
            },
            "funnel": {
                "dialed": out_curr, "connected": conn_curr,
                "conversations": conv_curr, "appts_set": appt_set_curr,
                "appts_met": appt_met_curr,
            },
            "handoffs": {
                "total": total_handoffs, "active": active_handoffs,
                "stale": len(stale_leads), "success_rate": handoff_success_rate,
                "by_agent": [{"agent": a, "count": c} for a, c in sorted(stale_by_agent.items(), key=lambda x: x[1], reverse=True)],
                "by_stage": [{"stage": s, "count": c} for s, c in sorted(stale_by_stage.items(), key=lambda x: x[1], reverse=True)],
            },
            "stale_leads": stale_leads[:50],
            "own_pipeline": {
                "total": len(fhalen_own_pipeline),
                "stages": [{"stage": s, "count": c} for s, c in sorted(fhalen_own_stages.items(), key=lambda x: x[1], reverse=True)],
                "stuck_in_lead": fhalen_stuck_in_lead,
                "stale_14d": len(fhalen_own_stale),
                "leads": fhalen_own_pipeline[:30],
            },
            "appt_summary_30d": {
                "total": total_appts_30d, "met": met_30d,
                "no_show": no_show_30d, "pending": pending_30d,
            },
            "insights": insights,
            "period": {
                "current": f"{since_curr.strftime('%b %d')} - {(until_curr - timedelta(days=1)).strftime('%b %d')}",
                "previous": f"{since_prev.strftime('%b %d')} - {(until_prev - timedelta(days=1)).strftime('%b %d')}",
            },
            "from_cache": False,
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        cache_set("isa", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/isa/tag-pending", methods=["POST"])
def api_tag_pending():
    """Add Fhalen_Pending tag to dropped-ball leads."""
    data = request.json or {}
    person_ids = data.get("person_ids", [])
    try:
        client = FUBClient()
        tagged = 0
        for pid in person_ids:
            try:
                client.add_tag(pid, config.DROPPED_BALL_TAG)
                tagged += 1
            except Exception:
                pass
        return jsonify({"success": True, "tagged": tagged})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/isa/tag-followup", methods=["POST"])
def api_tag_followup():
    """Add ISA_Followup tag to stale handoff leads."""
    data = request.json or {}
    person_ids = data.get("person_ids", [])
    try:
        client = FUBClient()
        tagged = 0
        for pid in person_ids:
            try:
                client.add_tag(pid, config.ISA_FOLLOWUP_TAG)
                tagged += 1
            except Exception:
                pass
        return jsonify({"success": True, "tagged": tagged})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/isa-transfers")
def api_isa_transfers():
    """ISA Transfer panel — query FUB directly by ISA_TRANSFER_FRESH tag, use DB for transfer dates."""
    import datetime as _dt
    api_key = os.environ.get("FUB_API_KEY", "")
    from fub_client import FUBClient
    client = FUBClient(api_key)
    from config import ISA_TRANSFER_WARM_STAGE, ISA_TRANSFER_FRESH_TAG, EXCLUDED_USERS

    # Pull DB records for transfer dates (person_id → row dict)
    db_rows = {r["person_id"]: r for r in (_db.get_all_isa_transfers() or [])}

    # Query FUB directly for everyone carrying the tag right now
    try:
        people = client.get_people(tag=ISA_TRANSFER_FRESH_TAG, limit=200)
    except Exception as e:
        return jsonify({"error": str(e), "agents": [], "totals": {"total": 0, "stage_changed": 0, "unchanged": 0}})

    if not people:
        return jsonify({"agents": [], "totals": {"total": 0, "stage_changed": 0, "unchanged": 0}})

    FUB_BASE = "https://yourfriendlyagent.followupboss.com/2/people/view/{}"
    now_utc = _dt.datetime.now(_dt.timezone.utc)
    # Names that should never appear in agent accountability panels
    excluded = {n.lower() for n in EXCLUDED_USERS}
    enriched = []
    for person in people:
        pid   = str(person.get("id", ""))
        stage = (person.get("stage") or "Lead").strip()
        stage_changed = stage not in ("Lead", ISA_TRANSFER_WARM_STAGE, "", "Unknown")

        agent_name = (person.get("assignedTo") or "Unassigned").strip() or "Unassigned"

        # Skip team leaders, ISA, and admins — they're not being held accountable here
        if agent_name.lower() in excluded:
            continue
        lead_name  = (person.get("name") or "Unknown").strip() or "Unknown"

        # Transfer date: prefer DB record, fall back to today
        db_row = db_rows.get(pid, {})
        if db_row.get("transfer_date"):
            td_str = db_row["transfer_date"]
            try:
                # transfer_date is stored as ISO string from Postgres
                td = _dt.datetime.fromisoformat(td_str.replace("Z", "+00:00"))
                if td.tzinfo is None:
                    td = td.replace(tzinfo=_dt.timezone.utc)
                days_since = (now_utc - td).days
            except Exception:
                days_since = 0
        else:
            days_since = 0
            # Opportunistically record in DB so future runs have a date
            try:
                _db.record_isa_transfer(pid, lead_name=lead_name, agent_name=agent_name)
            except Exception:
                pass

        enriched.append({
            "person_id":    pid,
            "lead_name":    lead_name,
            "agent_name":   agent_name,
            "days_since":   days_since,
            "stage":        stage,
            "stage_changed":stage_changed,
            "fub_url":      FUB_BASE.format(pid),
        })

    # Group by agent, sort each agent's leads by days_since desc (oldest first)
    by_agent = {}
    for t in enriched:
        by_agent.setdefault(t["agent_name"], []).append(t)
    for a in by_agent:
        by_agent[a].sort(key=lambda x: x["days_since"], reverse=True)

    agents = []
    for agent_name, leads in sorted(by_agent.items()):
        changed = sum(1 for l in leads if l["stage_changed"])
        agents.append({
            "agent_name":   agent_name,
            "total":        len(leads),
            "stage_changed":changed,
            "unchanged":    len(leads) - changed,
            "leads":        leads,
        })
    # Sort: most unchanged (needs work) first
    agents.sort(key=lambda x: x["unchanged"], reverse=True)

    total_changed   = sum(a["stage_changed"] for a in agents)
    total_unchanged = sum(a["unchanged"] for a in agents)
    return jsonify({
        "agents": agents,
        "totals": {
            "total":         len(enriched),
            "stage_changed": total_changed,
            "unchanged":     total_unchanged,
        }
    })


# ---- Appointment Accountability ----


def build_appointment_data():
    """Build appointment accountability data for the last 30 days."""
    client = FUBClient()
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    lookback = getattr(config, "APT_LOOKBACK_DAYS", 30)
    since = today - timedelta(days=lookback)
    ahead = today + timedelta(days=14)

    all_appts = client.get_appointments(since=since)

    # Resolve agent names — auto_detect_agents returns {name: user_dict}
    agents_dict = auto_detect_agents(client)
    agent_map = {}
    for name, user in agents_dict.items():
        agent_map[user.get("id")] = name
    agent_map[config.ISA_USER_ID] = config.ISA_NAME
    agent_map[config.MANAGER_USER_ID] = config.MANAGER_NAME
    agent_map[1] = "Barry Jenkins"

    # Process each appointment
    person_cache = {}
    appointments = []
    now = datetime.now(timezone.utc)

    for appt in all_appts:
        appt_id = appt.get("id")
        start_str = appt.get("start", "")
        outcome = appt.get("outcome")
        outcome_id = appt.get("outcomeId")
        created_by = appt.get("createdById")
        title = appt.get("title", "")
        invitees = appt.get("invitees", [])

        # Parse start time
        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        # Extract lead personId from invitees
        person_id = None
        lead_name = "Unknown"
        assigned_user_id = None
        assigned_agent = "Unknown"

        for inv in invitees:
            if inv.get("personId"):
                person_id = inv["personId"]
                lead_name = inv.get("name", "Unknown")
            if inv.get("userId") and inv["userId"] != created_by:
                assigned_user_id = inv["userId"]
                assigned_agent = inv.get("name", agent_map.get(inv["userId"], "Unknown"))

        # If no assigned agent found from invitees, try first userId that isn't ISA
        if not assigned_user_id:
            for inv in invitees:
                uid = inv.get("userId")
                if uid and uid != config.ISA_USER_ID:
                    assigned_user_id = uid
                    assigned_agent = inv.get("name", agent_map.get(uid, "Unknown"))
                    break

        # Fetch person details (cached)
        person_data = {}
        tags = []
        source = "Unknown"
        if person_id:
            if person_id not in person_cache:
                try:
                    person_cache[person_id] = client.get_person(person_id)
                except Exception:
                    person_cache[person_id] = {}
            person_data = person_cache[person_id]
            lead_name = person_data.get("name", lead_name)
            tags = person_data.get("tags", []) or []
            source = person_data.get("source", "Unknown") or "Unknown"

        # Skip excluded lead sources (e.g. Courted.io recruits)
        excluded_sources = getattr(config, "EXCLUDED_LEAD_SOURCES", [])
        if source in excluded_sources:
            continue

        # Calculate hours since appointment
        hours_since = (now - start_dt).total_seconds() / 3600
        is_past = hours_since > 0
        is_future = not is_past

        # Determine tier
        tier = None
        if is_past and not outcome:
            if hours_since >= config.APT_TIER3_HOURS:
                tier = "stale"
            elif hours_since >= config.APT_TIER2_HOURS:
                tier = "overdue"
            elif hours_since >= config.APT_TIER1_HOURS:
                tier = "pending"
            else:
                tier = "recent"

        appointments.append({
            "id": appt_id,
            "title": title,
            "start": start_str,
            "start_date": start_str[:10],
            "outcome": outcome,
            "outcome_id": outcome_id,
            "created_by": agent_map.get(created_by, f"User {created_by}"),
            "created_by_id": created_by,
            "assigned_agent": assigned_agent,
            "assigned_user_id": assigned_user_id,
            "lead_name": lead_name,
            "person_id": person_id,
            "stage": person_data.get("stage", "Unknown"),
            "source": source,
            "tags": tags,
            "hours_since": round(hours_since, 1),
            "days_since": round(hours_since / 24, 1),
            "is_past": is_past,
            "is_future": is_future,
            "tier": tier,
            "has_apt_set": config.APT_SET_TAG in tags,
            "has_outcome_needed": config.APT_OUTCOME_NEEDED_TAG in tags,
            "has_stale": config.APT_STALE_TAG in tags,
        })

    # Sort: past no-outcome first (worst), then by date
    appointments.sort(key=lambda a: (
        0 if a["tier"] == "stale" else 1 if a["tier"] == "overdue" else 2 if a["tier"] == "pending" else 3 if a["tier"] == "recent" else 4,
        -a["hours_since"],
    ))

    # Per-agent summary
    agent_summary = {}
    for a in appointments:
        if a["is_future"]:
            continue
        agent = a["assigned_agent"]
        if agent not in agent_summary:
            agent_summary[agent] = {
                "name": agent, "total": 0, "met": 0, "no_show": 0,
                "reschedule": 0, "no_outcome": 0, "stale": 0, "overdue": 0,
            }
        s = agent_summary[agent]
        s["total"] += 1
        if a["outcome"] == "Met with Client":
            s["met"] += 1
        elif a["outcome"] == "No show":
            s["no_show"] += 1
        elif a["outcome"] == "Reschedule Needed":
            s["reschedule"] += 1
        else:
            s["no_outcome"] += 1
            if a["tier"] == "stale":
                s["stale"] += 1
            elif a["tier"] == "overdue":
                s["overdue"] += 1

    for s in agent_summary.values():
        s["completion_rate"] = round((s["total"] - s["no_outcome"]) / s["total"] * 100) if s["total"] else 0

    # Sort agents worst-first
    agent_list = sorted(agent_summary.values(), key=lambda x: x["completion_rate"])

    # Totals
    past = [a for a in appointments if a["is_past"]]
    future = [a for a in appointments if a["is_future"]]
    total_met = sum(1 for a in past if a["outcome"] == "Met with Client")
    total_no_show = sum(1 for a in past if a["outcome"] == "No show")
    total_resched = sum(1 for a in past if a["outcome"] == "Reschedule Needed")
    total_no_outcome = sum(1 for a in past if not a["outcome"])
    total_stale = sum(1 for a in past if a["tier"] == "stale")
    completion_rate = round((len(past) - total_no_outcome) / len(past) * 100) if past else 0

    return {
        "appointments": appointments,
        "agents": agent_list,
        "totals": {
            "total_30d": len(past),
            "upcoming": len(future),
            "met": total_met,
            "no_show": total_no_show,
            "reschedule": total_resched,
            "no_outcome": total_no_outcome,
            "stale_7d": total_stale,
            "completion_rate": completion_rate,
        },
        "period": f"{since.strftime('%b %d')} - {(today - timedelta(days=1)).strftime('%b %d')}",
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "from_cache": False,
    }


@app.route("/api/appointments")
def api_appointments():
    """Appointment accountability tab."""
    force = request.args.get("force", "false").lower() == "true"
    try:
        if not force:
            cached = cache_get("appointments")
            if cached:
                cached["from_cache"] = True
                cached_at = cached.get("cached_at")
                if cached_at:
                    try:
                        age_mins = int((datetime.now(timezone.utc) - datetime.fromisoformat(cached_at)).total_seconds() / 60)
                        cached["cache_age"] = f"{age_mins // 60}h {age_mins % 60}m ago" if age_mins >= 60 else f"{age_mins}m ago"
                    except Exception:
                        pass
                return jsonify(cached)

        data = build_appointment_data()
        cache_set("appointments", data)
        return jsonify(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/appointments/sync-tags", methods=["POST"])
def api_sync_appointment_tags():
    """Run the tag lifecycle: APT_SET → APT_OUTCOME_NEEDED → APT_STALE → removed."""
    try:
        # Get fresh appointment data
        data = build_appointment_data()
        client = FUBClient()

        actions = {"tagged_set": 0, "tagged_needed": 0, "tagged_stale": 0,
                   "tags_removed": 0, "tasks_created": 0, "skipped": 0, "errors": 0}

        apt_tags = {config.APT_SET_TAG, config.APT_OUTCOME_NEEDED_TAG, config.APT_STALE_TAG}

        for appt in data["appointments"]:
            pid = appt.get("person_id")
            if not pid:
                actions["skipped"] += 1
                continue

            try:
                tags = list(appt.get("tags", []))
                has_any_apt_tag = bool(apt_tags & set(tags))
                outcome = appt.get("outcome")
                tier = appt.get("tier")

                # Outcome logged → remove all apt tags
                if outcome:
                    if has_any_apt_tag:
                        tags = [x for x in tags if x not in apt_tags]
                        client._request("PUT", f"people/{pid}", json_data={"tags": tags})
                        actions["tags_removed"] += 1
                    continue

                if not tier:
                    continue

                # Stale (7d+)
                if tier == "stale":
                    if config.APT_STALE_TAG not in tags:
                        tags = [t for t in tags if t not in apt_tags]
                        tags.append(config.APT_STALE_TAG)
                        client._request("PUT", f"people/{pid}", json_data={"tags": tags})
                        actions["tagged_stale"] += 1

                # Overdue (48h+)
                elif tier == "overdue":
                    if config.APT_OUTCOME_NEEDED_TAG not in tags:
                        tags = [t for t in tags if t not in apt_tags]
                        tags.append(config.APT_OUTCOME_NEEDED_TAG)
                        client._request("PUT", f"people/{pid}", json_data={"tags": tags})
                        actions["tagged_needed"] += 1
                        # Create task for the agent
                        if appt.get("assigned_user_id"):
                            try:
                                due = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
                                client.create_task(
                                    pid, appt["assigned_user_id"],
                                    config.APT_TASK_TEMPLATE.format(lead_name=appt["lead_name"]),
                                    due_date=due,
                                )
                                actions["tasks_created"] += 1
                            except Exception:
                                pass

                # Pending/Recent (<48h)
                elif tier in ("pending", "recent"):
                    if config.APT_SET_TAG not in tags:
                        tags = [t for t in tags if t not in apt_tags]
                        tags.append(config.APT_SET_TAG)
                        client._request("PUT", f"people/{pid}", json_data={"tags": tags})
                        actions["tagged_set"] += 1

            except Exception:
                actions["errors"] += 1
                continue

        # Clear cache so dashboard refreshes
        cache_clear("appointments")

        return jsonify({"success": True, "actions": actions})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _gather_hype_data(ai_text_count=None, ai_voice_count=None, human_isa=None):
    """
    Shared data-gathering for the hype email preview and send endpoints.

    ai_text_count / ai_voice_count: manually-entered lead counts from the
    dashboard (from MaverickRE's reporting). When provided, these override
    any FUB-side estimation. When None, both default to 0 — FUB's
    tag-based counting is unreliable (updatedSince doesn't mean 'tagged since').

    human_isa: manually-entered ISA appointment count (Fhalen). When provided,
    overrides the auto-detected fhalen_appts from the audit.

    Returns dict with: agents, period_label, ai_text_count, ai_voice_count,
                       fhalen_appts, fhalen_name, to_emails
    """
    from kpi_audit import count_appointments_for_user

    client = FUBClient()

    # ── 1. Audit data (cache → live fallback) ─────────────────────────────────
    audit = cache_get("audit")
    if not audit:
        try:
            audit = run_audit_data()
            cache_set("audit", audit)
        except Exception as _ae:
            logger.warning("hype-email: audit fetch failed: %s", _ae)
            audit = {"agents": [], "totals": {}, "period": {}, "thresholds": {}}

    agents = audit.get("agents", [])
    period = audit.get("period", {})
    period_label = f"{period.get('start','')} – {period.get('end','')}"

    # ── 2. AI lead counts — use manually-entered values; default 0 ────────────
    # FUB's updatedSince filter on /people is not reliable for counting
    # "tagged this week" — it returns people whose record was updated for any
    # reason, so totals balloon to thousands from historical tags.
    # Barry enters the real numbers from MaverickRE's dashboard.
    ai_text_count  = int(ai_text_count)  if ai_text_count  is not None else 0
    ai_voice_count = int(ai_voice_count) if ai_voice_count is not None else 0

    # ── 3. ISA appointments ───────────────────────────────────────────────────
    # If Barry manually entered a Human ISA count, trust that number.
    # Otherwise fall back to auto-detecting from Fhalen's audit metrics.
    fhalen_name = getattr(config, "LIVE_CALLS_ADMIN", "Fhalen")
    if human_isa is not None:
        fhalen_appts = int(human_isa)
    else:
        since_7d = datetime.now(timezone.utc) - timedelta(days=7)
        fhalen_appts = 0
        for a in agents:
            if a["name"].lower().startswith(fhalen_name.lower().split()[0].lower()):
                fhalen_appts = a["metrics"].get("appts_set", 0)
                break
        if fhalen_appts == 0:
            try:
                fhalen_user = client.get_user_by_name(fhalen_name)
                if fhalen_user:
                    since_dt = (datetime.fromisoformat(audit["period_since_iso"])
                                if "period_since_iso" in audit else since_7d)
                    until_dt = (datetime.fromisoformat(audit["period_until_iso"])
                                if "period_until_iso" in audit else datetime.now(timezone.utc))
                    all_appts = client.get_appointments(since=since_dt, until=until_dt)
                    fhalen_appts, _ = count_appointments_for_user(all_appts, fhalen_user["id"])
            except Exception as _fa:
                logger.warning("hype-email: fhalen appts fetch failed: %s", _fa)

    # ── 4. All FUB user emails ────────────────────────────────────────────────
    to_emails = client.get_all_user_emails()

    return {
        "agents":         agents,
        "period_label":   period_label,
        "ai_text_count":  ai_text_count,
        "ai_voice_count": ai_voice_count,
        "fhalen_appts":   fhalen_appts,
        "fhalen_name":    fhalen_name,
        "thresholds":     audit.get("thresholds", {}),
        "to_emails":      to_emails,
    }


@app.route("/api/preview-hype-email")
def api_preview_hype_email():
    """
    Render the hype email as a full HTML page — no email sent.
    Opens in a new browser tab so Barry can review before hitting Send.
    """
    from flask import make_response
    from email_report import build_hype_email as _build_hype
    try:
        ai_text   = request.args.get("ai_text",   0, type=int)
        ai_voice  = request.args.get("ai_voice",  0, type=int)
        human_isa = request.args.get("human_isa", None, type=int)
        data = _gather_hype_data(ai_text_count=ai_text, ai_voice_count=ai_voice, human_isa=human_isa)
        html = _build_hype(
            agents=data["agents"],
            period_label=data["period_label"],
            ai_text_count=data["ai_text_count"],
            ai_voice_count=data["ai_voice_count"],
            fhalen_appts=data["fhalen_appts"],
            fhalen_name=data["fhalen_name"],
            thresholds=data.get("thresholds", {}),
        )
        # Inject a preview banner so it's obvious this hasn't been sent yet
        banner = """
<div style="position:fixed;top:0;left:0;right:0;z-index:9999;background:#0f172a;
            color:#f59e0b;font-family:-apple-system,sans-serif;font-size:13px;
            font-weight:700;text-align:center;padding:10px 16px;
            display:flex;align-items:center;justify-content:center;gap:16px">
  <span>👁 PREVIEW — This email has NOT been sent</span>
  <span style="color:rgba(255,255,255,0.4);font-weight:400">
    Recipients: all FUB team members &nbsp;·&nbsp; Close this tab and click 🔥 Send to deliver
  </span>
</div>
<div style="height:44px"></div>
"""
        html = html.replace("<body>", "<body>" + banner, 1)
        resp = make_response(html, 200)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    except Exception as e:
        import traceback
        logger.error("preview-hype-email failed: %s\n%s", e, traceback.format_exc())
        return f"<pre style='color:red'>Error building preview:\n{e}</pre>", 500


@app.route("/api/send-hype-email", methods=["POST"])
def api_send_hype_email():
    """Send the weekly KPI hype email to the full team roster."""
    from email_report import send_hype_email as _send_hype
    try:
        body = request.get_json(silent=True) or {}
        ai_text   = int(body.get("ai_text",  0))
        ai_voice  = int(body.get("ai_voice", 0))
        human_isa = body.get("human_isa", None)
        human_isa = int(human_isa) if human_isa is not None else None
        data = _gather_hype_data(ai_text_count=ai_text, ai_voice_count=ai_voice, human_isa=human_isa)
        if not data["to_emails"]:
            return jsonify({"error": "Could not retrieve team emails from FUB"}), 500

        success, msg = _send_hype(
            agents=data["agents"],
            period_label=data["period_label"],
            ai_text_count=data["ai_text_count"],
            ai_voice_count=data["ai_voice_count"],
            fhalen_appts=data["fhalen_appts"],
            fhalen_name=data["fhalen_name"],
            to_emails=data["to_emails"],
            thresholds=data.get("thresholds", {}),
        )
        if success:
            logger.info("hype-email sent: %s", msg)
            agents = data["agents"]
            return jsonify({
                "success": True,
                "message": msg,
                "stats": {
                    "ai_text":     data["ai_text_count"],
                    "ai_voice":    data["ai_voice_count"],
                    "fhalen_appts": data["fhalen_appts"],
                    "total_leads": data["ai_text_count"] + data["ai_voice_count"] + data["fhalen_appts"],
                    "recipients":  len(data["to_emails"]),
                    "qualifiers":  sum(1 for a in agents if a["evaluation"]["overall_pass"]),
                },
            })
        return jsonify({"error": msg}), 500
    except Exception as e:
        import traceback
        logger.error("hype-email failed: %s\n%s", e, traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/send-appointment-email", methods=["POST"])
def api_send_appointment_email():
    """Send appointment accountability email."""
    try:
        from email_report import send_appointment_email
        body = request.get_json(silent=True) or {}
        subject_override = body.get("subject_override")
        data = build_appointment_data()
        success = send_appointment_email(data, subject_override=subject_override)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Send failed. Check SENDGRID_API_KEY."}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---- Bounce Sync: tag FUB leads whose email hard-bounced in SendGrid ----

def _fub_mark_email_unsubscribed(client, person, bounced_email):
    """
    Mark a specific email address as unsubscribed on a FUB person record.

    Sets isUnsubscribed=True on the matching email object so FUB shows it
    with a strikethrough and blocks it from all FUB automations / bulk email.
    Returns True if the record was updated, False if it was already unsubscribed
    or the address wasn't found on the person.
    """
    pid = person.get("id")
    emails = person.get("emails") or []
    updated = False
    for email_obj in emails:
        val = (email_obj.get("value") or "").strip().lower()
        if val == bounced_email.lower():
            if email_obj.get("isUnsubscribed"):
                return False  # already marked
            email_obj["isUnsubscribed"] = True
            updated = True
            break
    if updated:
        client._request("PUT", f"people/{pid}", json_data={"emails": emails})
        logger.info("sync-bounces: marked %s isUnsubscribed in FUB (pid=%s)", bounced_email, pid)
    return updated


def _fetch_sendgrid_bounces():
    """Fetch all hard bounces from SendGrid. Returns list of lowercase email strings."""
    import requests as _req
    from requests.exceptions import HTTPError as _HTTPError
    sg_key = os.environ.get("SENDGRID_API_KEY", "")
    if not sg_key:
        raise RuntimeError("SENDGRID_API_KEY not set")
    resp = _req.get(
        "https://api.sendgrid.com/v3/suppression/bounces",
        headers={"Authorization": f"Bearer {sg_key}", "Accept": "application/json"},
        params={"limit": 500},
        timeout=30,
    )
    try:
        resp.raise_for_status()
    except _HTTPError as http_err:
        if resp.status_code == 403:
            raise RuntimeError(
                "SendGrid API key is missing 'Suppression Management → Read Access' permission. "
                "Fix: SendGrid → Settings → API Keys → edit your key → add Suppression Management (Read Access)."
            ) from http_err
        raise
    return [b["email"].strip().lower() for b in resp.json() if b.get("email")]


@app.route("/api/sync-bounces", methods=["POST"])
def api_sync_bounces():
    """
    Fetch all hard bounces from SendGrid and for each matching FUB lead:
      1. Add BAD_EMAIL tag
      2. Mark the specific email address as isUnsubscribed=True in FUB

    Returns JSON summary: { bounces_found, tagged, already_tagged, not_in_fub, errors }
    """
    try:
        bounced_emails = _fetch_sendgrid_bounces()
    except Exception as sg_err:
        logger.error("sync-bounces: SendGrid fetch failed: %s", sg_err)
        return jsonify({"error": f"SendGrid error: {sg_err}"}), 500

    logger.info("sync-bounces: %d bounced addresses from SendGrid", len(bounced_emails))

    client = FUBClient()
    tagged = []
    already_tagged = []
    not_in_fub = []
    errors = []

    for email in bounced_emails:
        try:
            people = client.search_people_by_email(email)
            if not people:
                not_in_fub.append(email)
                continue
            for person in people:
                pid = person.get("id")
                raw_tags = person.get("tags") or []
                existing = [
                    (t.get("name") or t if isinstance(t, dict) else t)
                    for t in raw_tags
                ]
                already_done = config.BAD_EMAIL_TAG in existing
                if already_done:
                    already_tagged.append(email)
                else:
                    # Add BAD_EMAIL tag
                    client.add_tag_fast(pid, config.BAD_EMAIL_TAG, existing)
                    tagged.append(email)
                    logger.info("sync-bounces: tagged %s (pid=%s) BAD_EMAIL", email, pid)

                # Always try to mark isUnsubscribed in FUB (idempotent)
                _fub_mark_email_unsubscribed(client, person, email)

        except Exception as e:
            logger.warning("sync-bounces: error on %s: %s", email, e)
            errors.append(email)

    result = {
        "bounces_found": len(bounced_emails),
        "tagged": len(tagged),
        "already_tagged": len(already_tagged),
        "not_in_fub": len(not_in_fub),
        "errors": len(errors),
        "tagged_emails": tagged,
    }
    logger.info("sync-bounces complete: %s", result)
    return jsonify(result)


def scheduled_sync_bounces():
    """Scheduler wrapper for the nightly bounce sync."""
    try:
        with app.app_context():
            bounced_emails = _fetch_sendgrid_bounces()
            client = FUBClient()
            tagged_count = 0
            for email in bounced_emails:
                try:
                    people = client.search_people_by_email(email)
                    for person in people:
                        pid = person.get("id")
                        raw_tags = person.get("tags") or []
                        existing = [(t.get("name") or t if isinstance(t, dict) else t) for t in raw_tags]
                        if config.BAD_EMAIL_TAG not in existing:
                            client.add_tag_fast(pid, config.BAD_EMAIL_TAG, existing)
                            tagged_count += 1
                        # Mark email unsubscribed in FUB regardless of tag state
                        _fub_mark_email_unsubscribed(client, person, email)
                except Exception:
                    pass
            logger.info("scheduled_sync_bounces: tagged %d new BAD_EMAIL leads", tagged_count)
    except Exception as e:
        logger.error("scheduled_sync_bounces crashed: %s", e)


# ---- LeadStream: Dashboard ----

_ls_dashboard_cache = {"data": None, "time": None}
_run_jobs = {}  # job_id -> {status, results, error, started}

@app.route("/leadstream")
def leadstream_dashboard():
    return render_template("leadstream.html")


@app.route("/api/leadstream/dashboard")
def api_leadstream_dashboard():
    """Return LeadStream status, current tagged leads, and activity since last run.

    Uses FUB tag queries as the source of truth so the dashboard works even
    after a Railway restart wipes /tmp. The manifest (if present) provides
    enrichment data (scores, tiers, last_run timestamp).
    """
    import json as _json
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    # 3-minute cache to avoid hammering FUB API on every page refresh
    now = _dt.now(_tz.utc)
    cached = _ls_dashboard_cache
    if cached["data"] and cached["time"] and (now - cached["time"]).seconds < 180:
        return jsonify(cached["data"])

    # ── Load manifest (primary source of truth) ───────────────────────
    _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    _cache_base = (
        os.environ.get("LEADSTREAM_CACHE_DIR")
        or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
    )
    MANIFEST_FILE = os.path.join(_cache_base, "leadstream_manifest.json")
    manifest = None
    # Try file first (freshest), then Postgres (survives Railway restarts)
    try:
        with open(MANIFEST_FILE) as f:
            manifest = _json.load(f)
    except Exception:
        pass
    if not manifest and _db.is_available():
        try:
            manifest = _db.read_manifest()
        except Exception:
            pass
    if not manifest:
        manifest = {"agent": {}, "pond": []}

    last_run_str = manifest.get("last_run")
    last_run = None
    if last_run_str:
        try:
            last_run = _dt.fromisoformat(last_run_str)
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=_tz.utc)
        except Exception:
            pass

    # ── Get FUB client for activity tracking ─────────────────────────
    try:
        _client = FUBClient()
    except Exception:
        return jsonify({"error": "FUB API key not configured",
                        "totals": {"total": 0, "agent_leads": 0, "pond_leads": 0, "actioned": 0, "action_rate": 0},
                        "agents": {}, "pond": {"leads": [], "tagged": 0, "actioned": 0},
                        "last_run": last_run_str, "run_history": manifest.get("run_history", []),
                        "last_run_mode": manifest.get("last_run_mode", "full"), "api_warnings": []})

    # ── Fetch activity since last run ─────────────────────────────────
    calls_by_person = {}
    texts_by_person = {}
    api_warnings = []

    activity_since = last_run - _td(minutes=10) if last_run else now - _td(hours=4)
    try:
        for call in _client.get_calls(since=activity_since):
            if not call.get("isIncoming"):
                pid = call.get("personId")
                if pid:
                    calls_by_person[pid] = calls_by_person.get(pid, 0) + 1
    except Exception as e:
        msg = f"Could not fetch recent calls: {e}"
        logger.warning("Dashboard: %s", msg)
        api_warnings.append(msg)

    # Note: FUB textMessages endpoint rejects bulk requests without userId/personId filter.
    # Activity tracking uses calls only for "actioned" detection; texts are tracked in
    # lead_scoring.py per-agent where a userId is always available.

    # ── Build from manifest (accurate lead counts + scores/tiers) ─────
    def _item_to_lead(item):
        """Convert a manifest item dict to a dashboard lead dict."""
        pid = item.get("id") if isinstance(item, dict) else item
        name = (item.get("name", "") if isinstance(item, dict) else "") or f"ID:{pid}"
        return {
            "id": pid,
            "name": name,
            "score": item.get("score", 0) if isinstance(item, dict) else 0,
            "tier": item.get("tier", "") if isinstance(item, dict) else "",
            "stage": item.get("stage", "") if isinstance(item, dict) else "",
            "called": calls_by_person.get(pid, 0) > 0,
            "texted": texts_by_person.get(pid, 0) > 0,
            "actioned": calls_by_person.get(pid, 0) > 0 or texts_by_person.get(pid, 0) > 0,
        }

    agents_out = {}
    for agent_name, lead_items in manifest.get("agent", {}).items():
        enriched = [_item_to_lead(item) for item in lead_items]
        agents_out[agent_name] = {
            "leads": enriched,
            "tagged": len(enriched),
            "actioned": sum(1 for l in enriched if l["actioned"]),
        }

    pond_leads = [_item_to_lead(item) for item in manifest.get("pond", [])]
    pond_leads.sort(key=lambda l: l["score"], reverse=True)
    pond_actioned = sum(1 for l in pond_leads if l["actioned"])

    total_agent = sum(a["tagged"] for a in agents_out.values())
    total_actioned = sum(a["actioned"] for a in agents_out.values()) + pond_actioned
    grand_total = total_agent + len(pond_leads)

    result = {
        "last_run": last_run_str,
        "last_run_mode": manifest.get("last_run_mode", "full"),
        "run_history": manifest.get("run_history", []),
        "agents": agents_out,
        "pond": {
            "leads": pond_leads,
            "tagged": len(pond_leads),
            "actioned": pond_actioned,
        },
        "totals": {
            "agent_leads": total_agent,
            "pond_leads": len(pond_leads),
            "total": grand_total,
            "actioned": total_actioned,
            "action_rate": round(total_actioned / grand_total * 100, 1) if grand_total > 0 else 0,
        },
        "api_warnings": api_warnings,  # surfaced in dashboard if activity data is incomplete
    }

    # Only cache when we have real data — avoids caching an empty result
    # that could occur briefly after a Railway restart before the Volume mounts.
    if grand_total > 0:
        _ls_dashboard_cache["data"] = result
        _ls_dashboard_cache["time"] = now

        # ── Persist engagement snapshot for weekly tracker ────────────────
        # Keyed by last_run timestamp so each scoring run has one record.
        # Always overwrites so actioned counts improve as agents engage leads.
        if last_run_str:
            try:
                agents_snapshot = {
                    name: {"tagged": a["tagged"], "actioned": a["actioned"]}
                    for name, a in agents_out.items()
                }
                pond_snapshot = {"tagged": len(pond_leads), "actioned": pond_actioned}

                # Write to DB (primary)
                if _db.is_available():
                    _db.write_engagement_entries(
                        last_run_str,
                        manifest.get("last_run_mode", "full"),
                        agents_snapshot,
                        pond_snapshot,
                    )
                else:
                    # File fallback — dual-write to primary + /tmp/.cache
                    import tempfile as _tempfile
                    eng_log_path = os.path.join(_cache_base, "engagement_log.json")
                    eng_log = {}
                    for _rp in [eng_log_path, "/tmp/.cache/engagement_log.json"]:
                        try:
                            with open(_rp) as f:
                                eng_log = _json.load(f)
                            break
                        except Exception:
                            continue
                    eng_log[last_run_str] = {
                        "captured": now.isoformat(),
                        "mode": manifest.get("last_run_mode", "full"),
                        "agents": agents_snapshot,
                        "pond": pond_snapshot,
                        "total": grand_total,
                    }
                    cutoff = (now - _td(days=30)).isoformat()
                    eng_log = {k: v for k, v in eng_log.items() if k >= cutoff}
                    fd, tmp = _tempfile.mkstemp(dir=_cache_base, suffix=".tmp")
                    with os.fdopen(fd, "w") as f:
                        _json.dump(eng_log, f)
                    os.replace(tmp, eng_log_path)
                    if _cache_base != "/tmp/.cache":
                        try:
                            os.makedirs("/tmp/.cache", exist_ok=True)
                            _fb, _ft = _tempfile.mkstemp(dir="/tmp/.cache", suffix=".tmp")
                            with os.fdopen(_fb, "w") as f:
                                _json.dump(eng_log, f)
                            os.replace(_ft, "/tmp/.cache/engagement_log.json")
                        except Exception:
                            pass
            except Exception as e:
                logger.warning("Could not save engagement log: %s", e)

    return jsonify(result)


# ---- LeadStream: Weekly Engagement Tracker Backfill ----

@app.route("/api/leadstream/backfill-tracker", methods=["POST"])
def api_leadstream_backfill_tracker():
    """
    Backfill the engagement log for the past N days using the current manifest
    + real FUB call/text activity per day. Creates one entry per scheduled
    scoring run window (8am, 12pm, 4pm, 8pm ET) for each day.
    Only fills gaps — won't overwrite existing entries.
    """
    import json as _json, tempfile as _tmp
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    # ET offset: EDT = UTC-4, EST = UTC-5. Use fixed -4 (spring/summer) or detect.
    # Simple approach: use UTC-4 (EDT, Apr-Oct) vs UTC-5 (EST, Nov-Mar)
    _now_utc = _dt.now(_tz.utc)
    _et_offset = -4 if 3 <= _now_utc.month <= 10 else -5
    ET = _tz(offset=_td(hours=_et_offset))

    days_back = (request.json or {}).get("days", 4)

    _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    _cache_base = (
        os.environ.get("LEADSTREAM_CACHE_DIR")
        or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
    )
    os.makedirs(_cache_base, exist_ok=True)
    manifest_path = os.path.join(_cache_base, "leadstream_manifest.json")
    eng_log_path  = os.path.join(_cache_base, "engagement_log.json")

    # Load manifest — file first, then Postgres DB (survives Railway restarts)
    manifest = None
    for mp in [manifest_path, "/tmp/.cache/leadstream_manifest.json"]:
        try:
            with open(mp) as f:
                manifest = _json.load(f)
            break
        except Exception:
            continue
    if manifest is None and _db.is_available():
        try:
            manifest = _db.read_manifest()
        except Exception:
            pass
    if manifest is None:
        return jsonify({"error": "No manifest found — click Run Now first, then try backfill again"}), 404

    # Load existing engagement log — try primary path then /tmp/.cache fallback
    eng_log = {}
    for _rp in [eng_log_path, "/tmp/.cache/engagement_log.json"]:
        try:
            with open(_rp) as f:
                eng_log = _json.load(f)
            break
        except Exception:
            continue

    agents_manifest = manifest.get("agent", {})
    pond_manifest   = manifest.get("pond", [])

    # Scheduled run hours (ET)
    run_hours = [8, 12, 16, 20]
    now_utc = _dt.now(_tz.utc)

    # For each day from (days_back) ago up to yesterday, create entries
    entries_written = 0
    entries_skipped = 0

    for day_offset in range(days_back, 0, -1):
        # Day window in ET (using fixed UTC offset)
        day_et_start = (_dt.now(ET) - _td(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_et_end   = day_et_start + _td(days=1)
        day_utc_start = day_et_start.astimezone(_tz.utc)
        day_utc_end   = day_et_end.astimezone(_tz.utc)

        # Fetch all outbound calls for this day
        try:
            from lead_scoring import _get_leadstream_client
            _client = _get_leadstream_client()
            day_calls = _client.get_calls(since=day_utc_start, until=day_utc_end)
            calls_by_person = {}
            for call in day_calls:
                if not call.get("isIncoming"):
                    pid = call.get("personId")
                    if pid:
                        calls_by_person[pid] = calls_by_person.get(pid, 0) + 1
        except Exception as e:
            calls_by_person = {}
            logger.warning("Backfill: could not fetch calls for %s: %s", day_et_start.date(), e)

        # For each scheduled run window that day
        for hour in run_hours:
            run_et = day_et_start.replace(hour=hour, minute=7, second=0, microsecond=0)
            run_utc = run_et.astimezone(_tz.utc)

            # Don't write future entries
            if run_utc > now_utc:
                continue

            run_key = run_utc.isoformat()

            # Skip if already in log
            if run_key in eng_log:
                entries_skipped += 1
                continue

            # Build per-agent actioned counts (calls against their current manifest leads)
            agents_entry = {}
            for agent_name, lead_items in agents_manifest.items():
                tagged = len(lead_items)
                actioned = 0
                for item in lead_items:
                    pid = item.get("id") if isinstance(item, dict) else item
                    if pid and calls_by_person.get(pid, 0) > 0:
                        actioned += 1
                agents_entry[agent_name] = {"tagged": tagged, "actioned": actioned}

            pond_tagged   = len(pond_manifest)
            pond_actioned = sum(
                1 for item in pond_manifest
                if calls_by_person.get(item.get("id") if isinstance(item, dict) else item, 0) > 0
            )

            eng_log[run_key] = {
                "captured":  now_utc.isoformat(),
                "mode":      "backfill",
                "agents":    agents_entry,
                "pond":      {"tagged": pond_tagged, "actioned": pond_actioned},
                "total":     sum(a["tagged"] for a in agents_entry.values()) + pond_tagged,
            }
            entries_written += 1

    # Save — DB primary, file fallback
    if _db.is_available():
        db_written = _db.write_engagement_from_log_dict(eng_log)
        return jsonify({
            "success": True,
            "entries_written": entries_written,
            "entries_skipped": entries_skipped,
            "days_back": days_back,
            "total_log_entries": len(eng_log),
            "storage": "postgres",
            "db_rows_written": db_written,
        })
    else:
        cutoff = (now_utc - _td(days=30)).isoformat()
        eng_log = {k: v for k, v in eng_log.items() if k >= cutoff}
        fd, tmp2 = _tmp.mkstemp(dir=_cache_base, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            _json.dump(eng_log, f)
        os.replace(tmp2, eng_log_path)
        if _cache_base != "/tmp/.cache":
            try:
                os.makedirs("/tmp/.cache", exist_ok=True)
                _fb, _ft = _tmp.mkstemp(dir="/tmp/.cache", suffix=".tmp")
                with os.fdopen(_fb, "w") as f:
                    _json.dump(eng_log, f)
                os.replace(_ft, "/tmp/.cache/engagement_log.json")
            except Exception:
                pass
        return jsonify({
            "success": True,
            "entries_written": entries_written,
            "entries_skipped": entries_skipped,
            "days_back": days_back,
            "total_log_entries": len(eng_log),
            "storage": "file",
            "storage_path": eng_log_path,
        })


# ---- LeadStream: Storage Debug ----

@app.route("/api/debug/storage")
def api_debug_storage():
    """Verify which storage path is active and whether it's writable/persistent."""
    import json as _json, time as _time, tempfile as _tmp2
    _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    _cache_base = (
        os.environ.get("LEADSTREAM_CACHE_DIR")
        or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
    )
    results = {
        "cache_base": _cache_base,
        "env_LEADSTREAM_CACHE_DIR": os.environ.get("LEADSTREAM_CACHE_DIR"),
        "is_railway": _is_railway,
        "paths": {}
    }

    for label, path in [("primary", _cache_base), ("fallback", "/tmp/.cache")]:
        info = {"path": path, "exists": os.path.isdir(path), "writable": False,
                "engagement_log": None, "engagement_log_entries": None,
                "write_test": None}
        try:
            os.makedirs(path, exist_ok=True)
            test_file = os.path.join(path, f"_write_test_{int(_time.time())}.tmp")
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file)
            info["writable"] = True
        except Exception as e:
            info["write_test"] = str(e)

        eng_path = os.path.join(path, "engagement_log.json")
        if os.path.exists(eng_path):
            info["engagement_log"] = eng_path
            try:
                with open(eng_path) as f:
                    data = _json.load(f)
                info["engagement_log_entries"] = len(data)
                info["engagement_log_latest"] = max(data.keys()) if data else None
                info["engagement_log_oldest"] = min(data.keys()) if data else None
            except Exception as e:
                info["engagement_log_entries"] = f"error: {e}"
        results["paths"][label] = info

    results["db_available"] = _db.is_available()
    if _db.is_available():
        try:
            eng_from_db = _db.read_engagement_log(days=7)
            results["db_engagement_entries"] = len(eng_from_db)
        except Exception as e:
            results["db_engagement_entries"] = f"error: {e}"

    return jsonify(results)


# ============================================================
# GOALS  — agent setup, manager scorecard, FUB deal sync
# ============================================================

# ──────────────────────────────────────────────────────────────────────────────
# ICS calendar invite helper for prospecting blocks
# ──────────────────────────────────────────────────────────────────────────────

_ICAL_DAY_MAP = {
    "monday": "MO", "tuesday": "TU", "wednesday": "WE",
    "thursday": "TH", "friday": "FR", "saturday": "SA", "sunday": "SU",
}
_ICAL_WEEKDAY_NUM = {
    "monday": 0, "tuesday": 1, "wednesday": 2,
    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}


def _next_occurrence(day_name: str) -> "date":
    """Return the next calendar date that falls on day_name (today inclusive)."""
    from datetime import date, timedelta
    today = date.today()
    target_wd = _ICAL_WEEKDAY_NUM.get(day_name.lower(), 0)
    days_ahead = (target_wd - today.weekday()) % 7
    return today + timedelta(days=days_ahead)


def _duration_to_iso(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    parts = "PT"
    if h: parts += f"{h}H"
    if m: parts += f"{m}M"
    return parts


def _build_ics(agent_name: str, agent_email: str,
               days: list, start_time: str, duration_minutes: int,
               recurring: bool = True) -> str:
    """
    Build an ICS file (iCalendar).

    recurring=True  → RRULE weekly, deterministic UID (updates existing invite)
    recurring=False → One VEVENT per day this week, dated UIDs (single-week override)
    """
    import re as _re
    from datetime import date

    if not days:
        raise ValueError("No days specified for prospecting block ICS")

    days_sorted = sorted(days, key=lambda d: _ICAL_WEEKDAY_NUM.get(d.lower(), 0))
    next_dates   = {d: _next_occurrence(d) for d in days_sorted}

    hh, mm       = start_time.split(":")[:2]
    duration_iso = _duration_to_iso(duration_minutes)
    slug         = _re.sub(r"[^a-z0-9]", "-", agent_name.lower())

    day_labels   = {"monday":"Mon","tuesday":"Tue","wednesday":"Wed",
                    "thursday":"Thu","friday":"Fri","saturday":"Sat","sunday":"Sun"}
    days_readable = ", ".join(day_labels.get(d.lower(), d) for d in days_sorted)
    dur_label = _duration_to_iso(duration_minutes).replace("PT","").replace("H"," hr ").replace("M"," min").strip()
    desc  = (f"Prospecting Hour - your weekly commitment to building the business "
             f"that funds your goals. Block: {days_readable} at {start_time} ET "
             f"for {dur_label}. You chose this. Now protect it.")

    alarm = (
        "BEGIN:VALARM\r\n"
        "TRIGGER:-PT10M\r\n"
        "ACTION:DISPLAY\r\n"
        "DESCRIPTION:Time to prospect — your future self is counting on you.\r\n"
        "END:VALARM\r\n"
    )

    cal_header = (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "PRODID:-//Legacy Home Team//Goal System//EN\r\n"
        "CALSCALE:GREGORIAN\r\n"
        "METHOD:REQUEST\r\n"
    )

    if recurring:
        # Single VEVENT with RRULE — deterministic UID replaces existing invite
        first_day    = min(days_sorted, key=lambda d: next_dates[d])
        dtstart_date = next_dates[first_day]
        dtstart      = f"{dtstart_date.strftime('%Y%m%d')}T{hh}{mm}00"
        byday        = ",".join(_ICAL_DAY_MAP.get(d.lower(), "MO") for d in days_sorted)
        uid          = f"prospecting-block-{slug}@legacyhometeam.com"

        ics = (
            cal_header
            + "BEGIN:VEVENT\r\n"
            + f"UID:{uid}\r\n"
            + f"DTSTART;TZID=America/New_York:{dtstart}\r\n"
            + f"DURATION:{duration_iso}\r\n"
            + f"RRULE:FREQ=WEEKLY;BYDAY={byday}\r\n"
            + f"SUMMARY:Prospecting Hour - to accomplish my goals\r\n"
            + f"DESCRIPTION:{desc}\r\n"
            + f"ORGANIZER;CN=Barry Jenkins - Legacy Home Team:mailto:{config.EMAIL_FROM}\r\n"
            + f"ATTENDEE;RSVP=TRUE;CN={agent_name}:mailto:{agent_email}\r\n"
            + "STATUS:CONFIRMED\r\n"
            + "TRANSP:OPAQUE\r\n"
            + alarm
            + "END:VEVENT\r\n"
            + "END:VCALENDAR\r\n"
        )
    else:
        # One VEVENT per day — dated UIDs, no RRULE — single-week override
        vevents = ""
        for d in days_sorted:
            dt_date = next_dates[d]
            dtstart = f"{dt_date.strftime('%Y%m%d')}T{hh}{mm}00"
            uid     = f"prospecting-override-{slug}-{dt_date.strftime('%Y%m%d')}@legacyhometeam.com"
            vevents += (
                "BEGIN:VEVENT\r\n"
                + f"UID:{uid}\r\n"
                + f"DTSTART;TZID=America/New_York:{dtstart}\r\n"
                + f"DURATION:{duration_iso}\r\n"
                + f"SUMMARY:Prospecting Hour (this week) - to accomplish my goals\r\n"
                + f"DESCRIPTION:{desc}\r\n"
                + f"ORGANIZER;CN=Barry Jenkins - Legacy Home Team:mailto:{config.EMAIL_FROM}\r\n"
                + f"ATTENDEE;RSVP=TRUE;CN={agent_name}:mailto:{agent_email}\r\n"
                + "STATUS:CONFIRMED\r\n"
                + "TRANSP:OPAQUE\r\n"
                + alarm
                + "END:VEVENT\r\n"
            )
        ics = cal_header + vevents + "END:VCALENDAR\r\n"

    return ics


def _send_prospecting_ics(agent_name: str, agent_email: str,
                           days: list, start_time: str,
                           duration_minutes: int = 60,
                           recurring: bool = True) -> bool:
    """
    Generate an ICS calendar invite and email it to the agent via SendGrid.
    recurring=True  → weekly repeating template (sent from goal wizard)
    recurring=False → single-week override events (sent from /my-block page)
    Returns True on success.
    """
    import base64
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (Mail, Attachment, FileContent,
                                       FileName, FileType, Disposition)

    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key:
        print("[PROSPECTING ICS] No SENDGRID_API_KEY — skipping ICS send")
        return False

    ics_content = _build_ics(agent_name, agent_email, days, start_time,
                             duration_minutes, recurring=recurring)
    encoded     = base64.b64encode(ics_content.encode()).decode()

    first = agent_name.split()[0]
    day_labels = {"monday":"Mon","tuesday":"Tue","wednesday":"Wed",
                  "thursday":"Thu","friday":"Fri","saturday":"Sat"}
    days_readable = ", ".join(day_labels.get(d, d) for d in sorted(
        days, key=lambda d: _ICAL_WEEKDAY_NUM.get(d.lower(), 0)))

    hh, mm = start_time.split(":")[:2]
    h_int = int(hh)
    ampm  = "AM" if h_int < 12 else "PM"
    h12   = h_int % 12 or 12
    time_readable = f"{h12}:{mm} {ampm}"

    dur_map = {30:"30 minutes", 60:"1 hour", 90:"1 hour 30 minutes", 120:"2 hours"}
    dur_label = dur_map.get(duration_minutes, f"{duration_minutes} minutes")

    subject = (f"📆 Your Prospecting Time Block is locked in, {first}!"
               if recurring else
               f"📅 Updated block for this week, {first} — calendar invite inside")
    html_body = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            max-width:560px;margin:0 auto;background:#080c14;color:#e8edf8;
            border-radius:10px;overflow:hidden">
  <div style="background:linear-gradient(135deg,#0f1520,#131d30);
              padding:32px 28px;border-bottom:1px solid #243050">
    <div style="font-size:11px;font-weight:700;letter-spacing:.12em;
                text-transform:uppercase;color:#f5a623;margin-bottom:6px">
      Legacy Home Team — Goal System
    </div>
    <div style="font-size:24px;font-weight:800;line-height:1.25;margin-bottom:8px">
      Your prospecting block is official, {first}. 🎯
    </div>
    <div style="font-size:15px;color:#94a3b8;line-height:1.6">
      You committed to showing up. Here's your calendar invite — accept it and
      protect that time like it's a listing appointment.
    </div>
  </div>

  <div style="padding:24px 28px">
    <div style="background:#0f1520;border:1px solid #243050;border-radius:10px;
                padding:20px;margin-bottom:20px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;
                  text-transform:uppercase;color:#68789a;margin-bottom:12px">
        Your Schedule
      </div>
      <div style="font-size:18px;font-weight:700;color:#f5a623;margin-bottom:4px">
        Prospecting Hour — to accomplish my goals
      </div>
      <div style="font-size:14px;color:#e8edf8;margin-bottom:2px">
        📅 Every week: <strong>{days_readable}</strong>
      </div>
      <div style="font-size:14px;color:#e8edf8;margin-bottom:2px">
        🕗 Starting at: <strong>{time_readable} ET</strong>
      </div>
      <div style="font-size:14px;color:#e8edf8">
        ⏱ Duration: <strong>{dur_label}</strong>
      </div>
    </div>

    <div style="background:rgba(245,166,35,.08);border:1px solid rgba(245,166,35,.2);
                border-radius:8px;padding:16px;margin-bottom:20px;font-size:14px;
                color:#e8edf8;line-height:1.6">
      <strong style="color:#f5a623">Why this matters:</strong> The calendar invite
      is attached below. Accept it and your phone will remind you 10 minutes before
      every session. The agents who hit their goals aren't smarter — they're just
      harder to move off their schedule.
    </div>

    <div style="font-size:13px;color:#68789a;line-height:1.6">
      This is a recurring invite. It will show up every week until you remove it.
      If your schedule changes, just text Barry and we'll update it.
    </div>
  </div>

  <div style="padding:16px 28px 24px;border-top:1px solid #243050;font-size:12px;
              color:#3d506e;text-align:center">
    Barry Jenkins · Legacy Home Team · LPT Realty<br>
    (757) 919-8874 · legacyhomesearch.com
  </div>
</div>
"""
    text_body = (
        f"Your prospecting block is locked in, {first}!\n\n"
        f"Schedule: {days_readable} at {time_readable} ET, {dur_label}\n\n"
        f"The calendar invite is attached. Accept it so your phone reminds you "
        f"10 minutes before every session.\n\n"
        f"— Barry Jenkins, Legacy Home Team"
    )

    msg = Mail(
        from_email=config.EMAIL_FROM,
        to_emails=agent_email,
        subject=subject,
        plain_text_content=text_body,
        html_content=html_body,
    )
    att = Attachment(
        FileContent(encoded),
        FileName("prospecting-block.ics"),
        FileType("text/calendar"),
        Disposition("attachment"),
    )
    msg.attachment = att

    try:
        SendGridAPIClient(sg_key).send(msg)
        return True
    except Exception as e:
        print(f"[PROSPECTING ICS] SendGrid error: {e}")
        raise


# ── Re-send prospecting block invite from dashboard ───────────────────────────

@app.route("/api/goals/prospecting-block/<token>", methods=["POST"])
def api_save_prospecting_block(token):
    """
    Save or update an agent's prospecting block from their dashboard.
    Accepts: {days, start_time, duration_minutes, send_invite: bool}
    Also used for re-sending calendar invites.
    """
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return jsonify({"error": "Invalid or expired link"}), 403

    body        = request.json or {}
    resend_only = body.get("resend_only", False)
    # recurring=True  → update the weekly template (default for wizard + dashboard)
    # recurring=False → override just this coming week (from /my-block page)
    recurring   = bool(body.get("recurring", True))

    if resend_only:
        # Re-send invite using existing block from DB
        existing_block = _db.get_prospecting_block(agent_name)
        if not existing_block or not existing_block.get("prospecting_days"):
            return jsonify({"error": "No prospecting block on file to re-send"}), 400
        days     = existing_block["prospecting_days"]
        start    = existing_block["start_time"]
        duration = existing_block["duration_minutes"]
        record   = existing_block
    else:
        days     = body.get("days") or []
        start    = body.get("start_time", "09:00")
        duration = int(body.get("duration_minutes", 60))
        if not days:
            return jsonify({"error": "Please choose at least one day"}), 400
        # Only update the DB template when it's a recurring change
        if recurring:
            record = _db.upsert_prospecting_block(
                agent_name=agent_name,
                prospecting_days=days,
                start_time=start,
                duration_minutes=duration,
            )
        else:
            # Single-week override — don't overwrite the recurring template
            record = _db.get_prospecting_block(agent_name) or {}

    invite_sent = False
    if body.get("send_invite", True):
        _all_profiles = {p["agent_name"]: p for p in (_db.get_agent_profiles(active_only=False) or [])}
        ap    = _all_profiles.get(agent_name, {})
        email = ap.get("email", "")
        if email:
            try:
                _send_prospecting_ics(agent_name, email, days, start, duration,
                                      recurring=recurring)
                if recurring:
                    _db.mark_prospecting_invite_sent(agent_name)
                invite_sent = True
            except Exception as e:
                print(f"[PROSPECTING ICS] Send failed for {agent_name}: {e}")
        else:
            print(f"[PROSPECTING ICS] No email on file for {agent_name}")

    return jsonify({"success": True, "record": record, "invite_sent": invite_sent})


@app.route("/api/goals/prospecting-block/<token>", methods=["GET"])
def api_get_prospecting_block(token):
    """Return the agent's current prospecting block schedule."""
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return jsonify({"error": "Invalid or expired link"}), 403
    block = _db.get_prospecting_block(agent_name)
    return jsonify({"success": True, "block": block})


# ---- Option B: Update prospecting block for the week ──────────────────────

@app.route("/my-block/<token>")
def my_block_page(token):
    """Mobile-optimized page for agents to update their weekly call block."""
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return ("<h2 style='font-family:sans-serif;padding:2rem;color:#fff;background:#0f172a;min-height:100vh'>"
                "This link has expired or is invalid. Ask Barry for a new one.</h2>"), 404
    return render_template("my_block.html", token=token, agent_name=agent_name)


# ---- Agent self-service setup page (token link) ----

@app.route("/goals/setup/<token>")
def goals_setup_page(token):
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return "<h2 style='font-family:sans-serif;padding:2rem'>This link has expired or is invalid. Ask Barry for a new one.</h2>", 404
    existing      = _db.get_goal(agent_name, year=datetime.now().year)
    existing_why  = _db.get_agent_why(agent_name)
    existing_ident= _db.get_agent_identity(agent_name)
    profiles      = {p["agent_name"]: p for p in _db.get_agent_profiles(active_only=False)}
    existing_profile = profiles.get(agent_name, {})
    return render_template("goal_setup.html",
                           agent_name=agent_name, token=token,
                           goal=existing, year=datetime.now().year,
                           existing_why=existing_why,
                           existing_identity=existing_ident,
                           existing_profile=existing_profile)


@app.route("/api/goals/setup/<token>", methods=["POST"])
def api_goals_setup_save(token):
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return jsonify({"error": "Invalid or expired link"}), 403
    body = request.json or {}
    try:
        year = int(body.get("year", datetime.now().year))

        # Goal quality validation
        gci = float(body.get("gci_goal", 0))
        quality_flags = []
        if gci > 0:
            if gci < 30000:
                quality_flags.append(f"GCI goal of ${gci:,.0f} seems very low — is this intentional?")
            if gci > 1000000:
                quality_flags.append(f"GCI goal of ${gci:,.0f} is ambitious — great, but make sure your activity targets reflect it.")
            # Flag if agent already has a goal and is changing it by >50%
            existing = _db.get_goal(agent_name)
            if existing and float(existing.get("gci_goal", 0)) > 0:
                old_gci = float(existing["gci_goal"])
                change_pct = abs(gci - old_gci) / old_gci * 100
                if change_pct > 50:
                    quality_flags.append(f"Goal changed by {change_pct:.0f}% — archiving previous goal of ${old_gci:,.0f}.")
        # Log flags for Barry visibility (non-blocking)
        if quality_flags:
            print(f"[GOAL QA] {agent_name}: {'; '.join(quality_flags)}")
            # TODO: surface these on manager scorecard

        goal = _db.save_goal_with_history(
            agent_name=agent_name,
            year=year,
            gci_goal=float(body["gci_goal"]),
            avg_sale_price=float(body.get("avg_sale_price", 400000)),
            commission_pct=float(body.get("commission_pct", 0.025)),
            soi_closings_expected=int(body.get("soi_closings_expected", 0)),
            soi_gci_expected=float(body.get("soi_gci_expected", 0)),
            sphere_touch_monthly=int(body.get("sphere_touch_monthly", 2)),
            contact_rate=float(body.get("contact_rate", 0.15)),
            call_to_appt_rate=float(body.get("call_to_appt_rate", 0.10)),
            appt_to_contract_rate=float(body.get("appt_to_contract_rate", 0.30)),
            contract_to_close_rate=float(body.get("contract_to_close_rate", 0.80)),
            set_by="agent",
            notes=body.get("notes"),
        )
        # Save why
        why = body.get("why") or {}
        if why.get("why_statement") or why.get("who_benefits"):
            _db.upsert_agent_why(
                agent_name=agent_name,
                why_statement=why.get("why_statement"),
                who_benefits=why.get("who_benefits"),
                who_benefits_custom=why.get("who_benefits_custom"),
                what_happens=why.get("what_happens"),
            )
        # Save identity
        ident = body.get("identity") or {}
        if ident.get("identity_archetype"):
            _db.upsert_agent_identity(
                agent_name=agent_name,
                identity_archetype=ident.get("identity_archetype"),
                custom_identity=ident.get("custom_identity"),
                power_hour_time=ident.get("power_hour_time"),
                daily_calls_target=ident.get("daily_calls_target"),
                daily_texts_target=ident.get("daily_texts_target"),
                daily_appts_target=ident.get("daily_appts_target"),
            )
        # Save contact info (phone + email — agent-provided wins over FUB)
        contact = body.get("contact") or {}
        phone = (contact.get("phone") or "").strip()
        email = (contact.get("email") or "").strip()
        if phone or email:
            _db.upsert_agent_profile(
                agent_name=agent_name,
                phone=phone or None,
                email=email or None,
            )
        # ── Save prospecting block + send calendar invites ─────────────
        pb = body.get("prospecting_block") or {}
        pb_days     = pb.get("days") or []
        pb_start    = pb.get("start_time", "09:00")
        pb_duration = int(pb.get("duration_minutes", 60))
        pb_record   = None
        if pb_days:
            pb_record = _db.upsert_prospecting_block(
                agent_name=agent_name,
                prospecting_days=pb_days,
                start_time=pb_start,
                duration_minutes=pb_duration,
            )
            # Get agent email to send ICS
            _all_p_dict  = {p["agent_name"]: p for p in (_db.get_agent_profiles(active_only=False) or [])}
            _ap          = _all_p_dict.get(agent_name, {})
            _agent_email = (contact.get("email") or "").strip() or _ap.get("email", "")
            if _agent_email and pb_record:
                try:
                    _send_prospecting_ics(
                        agent_name=agent_name,
                        agent_email=_agent_email,
                        days=pb_days,
                        start_time=pb_start,
                        duration_minutes=pb_duration,
                    )
                    _db.mark_prospecting_invite_sent(agent_name)
                    print(f"[PROSPECTING ICS] Sent calendar invites to {_agent_email} for {agent_name}")
                except Exception as _ics_err:
                    print(f"[PROSPECTING ICS] Failed to send ICS to {_agent_email}: {_ics_err}")
        # ─────────────────────────────────────────────────────────────────

        # ── Notify Barry ───────────────────────────────────────────────
        try:
            is_first = not (existing and float(existing.get("gci_goal", 0)) > 0)
            action   = "set their goals for the first time" if is_first else "updated their goals"
            gci_fmt  = f"${float(body['gci_goal']):,.0f}"
            why_stmt = (body.get("why") or {}).get("why_statement", "")
            who_ben  = (body.get("why") or {}).get("who_benefits", "")
            first    = agent_name.split()[0]
            lines    = [f"{agent_name} just {action}.", f"", f"GCI Goal: {gci_fmt}"]
            if why_stmt:
                lines.append(f"Why: {why_stmt}")
            if who_ben:
                lines.append(f"Who they're doing it for: {who_ben}")
            if pb_days:
                day_labels = {"monday":"Mon","tuesday":"Tue","wednesday":"Wed",
                              "thursday":"Thu","friday":"Fri","saturday":"Sat"}
                days_str = ", ".join(day_labels.get(d, d) for d in pb_days)
                lines.append(f"Prospecting Block: {days_str} at {pb_start} ({pb_duration} min)")
            if quality_flags:
                lines.append(f"")
                lines.append(f"⚠️ Flags: {'; '.join(quality_flags)}")
            lines += ["", f"— Legacy Home Team Dashboard"]
            notify_body = "\n".join(lines)
            notify_html = notify_body.replace("\n", "<br>")
            subj = f"🎯 {first} {'set their goals' if is_first else 'updated their goals'} — {gci_fmt}"

            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
            sg_key = os.environ.get("SENDGRID_API_KEY")
            if sg_key:
                _msg = Mail(
                    from_email=config.EMAIL_FROM,
                    to_emails=config.EMAIL_FROM,
                    subject=subj,
                    plain_text_content=notify_body,
                    html_content=f"<div style='font-family:sans-serif;font-size:15px;line-height:1.7;color:#222;max-width:500px;margin:24px auto'>{notify_html}</div>",
                )
                SendGridAPIClient(sg_key).send(_msg)
                print(f"[GOAL NOTIFY] Sent Barry notification for {agent_name} goal {'set' if is_first else 'update'}")
        except Exception as _ne:
            print(f"[GOAL NOTIFY] Failed to notify Barry: {_ne}")
        # ───────────────────────────────────────────────────────────────

        return jsonify({"success": True, "goal": goal,
                        "prospecting_block": pb_record})
    except (KeyError, ValueError) as e:
        return jsonify({"error": f"Bad input: {e}"}), 400


# ── Agent self-serve daily dashboard ─────────────────────────────────────────

@app.route("/my-goals/<token>")
def agent_dashboard_page(token):
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return "<h2 style='font-family:sans-serif;padding:2rem'>This link has expired or is invalid. Ask Barry for a new one.</h2>", 404
    return render_template("agent_dashboard.html",
                           agent_name=agent_name, token=token,
                           year=datetime.now().year)


@app.route("/api/goals/my-goals/<token>")
def api_agent_dashboard(token):
    """Return everything an agent needs to render their personal dashboard."""
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return jsonify({"error": "Invalid token"}), 403
    year = datetime.now().year
    goal     = _db.get_goal(agent_name, year=year)
    why      = _db.get_agent_why(agent_name)
    ident    = _db.get_agent_identity(agent_name)
    streak   = _db.get_streak(agent_name)
    today_act= _db.get_todays_activity(agent_name)
    activity = _db.get_daily_activity(agent_name, days=60)

    targets  = _db.compute_targets(goal) if goal else {}
    actuals  = {}
    if goal:
        ytd = _db.get_ytd_cache(year=year)
        a   = ytd.get(agent_name, {})
        deal_summary = _db.get_deal_summary(agent_name, year=year)
        actuals = {
            "calls_ytd":   a.get("calls_ytd", 0),
            "convos_ytd":  a.get("convos_ytd", 0),
            "appts_ytd":   a.get("appts_ytd", 0),
            "closings_ytd": deal_summary.get("closings", 0),
        }
    _profile    = next((p for p in _db.get_agent_profiles(active_only=False)
                        if p["agent_name"] == agent_name), {})
    _start_date = _profile.get("start_date")
    pace = _db.compute_pace(goal, targets, actuals, start_date=_start_date) if goal else {}

    return jsonify({
        "agent_name":  agent_name,
        "year":        year,
        "goal":        goal,
        "why":         why,
        "identity":    ident,
        "streak":      streak,
        "today":       today_act,
        "activity_log": activity,
        "targets":     targets,
        "actuals":     actuals,
        "pace":        pace,
        "start_date":  _start_date,
    })


@app.route("/api/goals/my-goals/<token>/hero")
def api_agent_hero(token):
    """
    Return a dynamic hero banner message for the agent's personal dashboard.
    Mirrors the arc/vibe of their most recent morning nudge, layered with
    current streak, pace, and situation for maximum relevance.
    """
    agent_name = _db.resolve_goal_token(token)
    if not agent_name:
        return jsonify({"error": "Invalid token"}), 403

    import hashlib as _hashlib, random as _rng_mod
    year       = datetime.now().year
    goal       = _db.get_goal(agent_name, year=year)
    streak_d   = _db.get_streak(agent_name) or {}
    today_act  = _db.get_todays_activity(agent_name) or {}
    recent_arcs = _db.get_recent_arcs(agent_name, days=3)
    last_arc   = recent_arcs[0] if recent_arcs else None

    # Pace calculation (mirrors api_agent_dashboard)
    pace_status = "gray"
    pace_pct    = 0.0
    if goal:
        ytd    = _db.get_ytd_cache(year=year)
        a      = ytd.get(agent_name, {})
        deal_sum = _db.get_deal_summary(agent_name, year=year)
        actuals = {
            "calls_ytd":    a.get("calls_ytd", 0),
            "appts_ytd":    a.get("appts_ytd", 0),
            "closings_ytd": deal_sum.get("closings", 0),
        }
        targets    = _db.compute_targets(goal)
        pace       = _db.compute_pace(goal, targets, actuals)
        pace_status = pace.get("overall_status", "gray")
        pace_pct    = float(pace.get("overall_pct", 0))

    current_streak = int(streak_d.get("current_streak", 0))

    # Deterministic-per-day seed so the hero is stable if they reload
    _seed = int(_hashlib.md5(
        f"{agent_name}{datetime.now().date().isoformat()}".encode()
    ).hexdigest(), 16) % 10_000_000
    rng = _rng_mod.Random(_seed)

    # ── Arc-specific hero copy (primary driver = today's email arc) ─────────
    _arc_heroes = {
        "identity": [
            ("THIS IS WHO YOU ARE.",
             "Show up. Do the work. Let the identity take care of the rest.",
             "gold", "⚡"),
            ("THE CONSISTENT ONE SHOWS UP.",
             "Not sometimes. Not when motivated. Every single day.",
             "gold", "💎"),
        ],
        "purpose": [
            ("REMEMBER WHY YOU STARTED.",
             "Your why is bigger than any bad week — and bigger than your best one.",
             "purple", "🎯"),
            ("THE WORK IS THE POINT.",
             "Every call is a chance to change someone's life. Including yours.",
             "purple", "🔑"),
        ],
        "scoreboard": [
            ("THE BOARD DOESN'T LIE.",
             "Here's where you stand. The agents above you aren't smarter. They just make more calls.",
             "blue", "📊"),
            ("YOUR RANK IS A CHOICE.",
             "Every missed call is a vote for someone else's spot.",
             "blue", "📈"),
        ],
        "compound": [
            ("SMALL DAYS BUILD BIG YEARS.",
             "20 calls today × 250 days = 5,000 conversations. You're not skipping a call. You're skipping 100 chances.",
             "blue", "🧱"),
            ("THE MATH WORKS IF YOU DO.",
             "Consistency isn't exciting. It's just the only thing that actually works.",
             "blue", "📐"),
        ],
        "comeback": [
            ("EVERY CHAMPION HAS THIS CHAPTER.",
             "The best agents on this team have all been here. The question is what you do next.",
             "yellow", "🏆"),
            ("THIS IS WHERE IT TURNS AROUND.",
             "Not next week. Not Monday. Right now, with the next call you make.",
             "yellow", "↩️"),
        ],
        "elite": [
            ("TOP OF THE BOARD. DEFEND IT.",
             "You've earned your spot. Now show everyone it wasn't an accident.",
             "green", "🥇"),
            ("ELITE ISN'T A TITLE. IT'S A DECISION.",
             "Made daily. One call at a time.",
             "green", "⚡"),
        ],
        "deal_math": [
            ("THE MATH IS SIMPLE. THE DOING ISN'T.",
             "Every call you skip is a pitch you let go by. You don't know how many you'll get. Swing.",
             "gold", "⚾"),
            ("KNOW YOUR NUMBER. HIT YOUR NUMBER.",
             "The agents who close the most aren't smarter. They just tracked the math and did the work.",
             "gold", "🔢"),
        ],
    }
    _default_heroes = [
        ("LET'S GET TO WORK.", "Your team is counting. So is that family that needs to sell their house.", "gold", "⚡"),
        ("MAKE YOUR CALLS.", "The simplest instruction. The hardest habit. The only thing that changes the year.", "gold", "📞"),
        ("THE WORK IS THE ANSWER.", "Every problem you're facing right now has the same solution: make the next call.", "gold", "💡"),
    ]

    if last_arc and last_arc in _arc_heroes:
        headline, subline, color, emoji = rng.choice(_arc_heroes[last_arc])
    else:
        headline, subline, color, emoji = rng.choice(_default_heroes)

    # ── Situational overlays (evaluated highest-priority-first) ─────────────
    if current_streak >= 10:
        headline = f"🔥 {current_streak} DAYS STRAIGHT."
        subline  = "You're building something most agents only talk about. Don't stop now."
        color = "gold"; emoji = "🔥"
    elif current_streak >= 5:
        headline = f"{current_streak} DAYS IN A ROW."
        subline  = "Streak agents outsell everyone else — not because they're smarter, but because they show up."
        color = "gold"; emoji = "🔥"

    if pace_status == "green" and pace_pct >= 110:
        headline = "AHEAD OF PACE."
        subline  = "You're running hot. This is what a great year feels like. Stay locked in."
        color = "green"; emoji = "✅"
    elif pace_status == "red" and pace_pct < 60:
        headline = "TIME TO CLOSE THE GAP."
        subline  = "Behind pace, but the year isn't over. It closes today — one call at a time."
        color = "red"; emoji = "🎯"

    return jsonify({"headline": headline, "subline": subline, "color": color, "emoji": emoji, "arc": last_arc})


    # Manual activity logging removed — numbers auto-sync from FUB nightly.


# ---- Manager goals dashboard ----

@app.route("/goals")
def goals_dashboard():
    # Goals are now embedded in the main dashboard — redirect to /
    from flask import redirect
    return redirect("/")


# ---- Scan FUB for agents and seed agent_profiles ----

@app.route("/api/goals/scan-agents", methods=["POST"])
def api_goals_scan_agents():
    """Pull agent roster from FUB, upsert into agent_profiles, return list."""
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503
    try:
        client = FUBClient()
        excluded = list(config.EXCLUDED_USERS)
        agents = client.get_agents_with_email(excluded_names=excluded)
        for a in agents:
            _db.upsert_agent_profile(a["name"], fub_user_id=a["fub_user_id"],
                                     email=a["email"], is_active=True)
        return jsonify({"success": True, "agents": agents, "count": len(agents)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---- Time-block nudge email ----

def _build_time_block_email_html(first_name: str, block_url: str, goal: dict | None) -> tuple[str, str]:
    """
    Build a personalized time-block nudge email in Barry's voice.
    Returns (subject, html_body).
    """
    # ── Subject line ──────────────────────────────────────────────────────
    if goal and goal.get("gci_goal"):
        gci = int(goal["gci_goal"])
        gci_fmt = f"${gci:,.0f}".replace(",000", "K") if gci >= 1000 else f"${gci:,}"
        subject = f"{first_name} — your {gci_fmt} year needs this one thing"
    else:
        subject = f"{first_name} — can we talk about your schedule?"

    # ── Goal math section (only if goal exists) ───────────────────────────
    if goal and goal.get("gci_goal"):
        targets = _db.compute_targets(goal)
        closings = targets.get("closings_needed", 0)
        wk_convos = targets.get("convos_needed_wk", 0)
        gci = int(goal["gci_goal"])
        gci_str = f"${gci:,}"
        goal_block = f"""
        <p style="margin:0 0 18px">You set a goal of <strong>{gci_str}</strong> this year.
        Working backwards, that&rsquo;s roughly <strong>{closings:.0f} closings</strong> &mdash;
        which means your model says you need about
        <strong>{wk_convos:.0f} real conversations a week</strong> with new people to stay on track.</p>

        <p style="margin:0 0 18px">That math doesn&rsquo;t happen by accident.
        It happens by appointment &mdash; with yourself.</p>
"""
    else:
        goal_block = """
        <p style="margin:0 0 18px">Even without a number on paper yet, the principle is the same:
        the conversations that build your year don&rsquo;t just happen.
        You have to protect the time before someone else fills it.</p>
"""

    # ── Full HTML ─────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Georgia,'Times New Roman',serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 16px">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;width:100%">

  <!-- Header bar -->
  <tr><td style="background:#1a1a2e;padding:20px 36px">
    <p style="margin:0;font-size:13px;color:#8888aa;font-family:Arial,sans-serif;letter-spacing:.08em;text-transform:uppercase">Legacy Home Team</p>
    <p style="margin:4px 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif">From Barry Jenkins</p>
  </td></tr>

  <!-- Body -->
  <tr><td style="padding:36px 36px 28px">
    <p style="margin:0 0 20px;font-size:17px;color:#111;line-height:1.65">{first_name},</p>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">I was looking at the team schedule this week.</p>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">Nobody&rsquo;s got their prospecting block set yet.</p>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">I&rsquo;m not saying that to call anyone out &mdash;
    I&rsquo;m saying it because I&rsquo;ve watched this pattern play out long enough to know:
    that one thing is what separates the agents who hit their number from the ones who get to November
    wondering where the year went.</p>

    <div style="font-size:16px;color:#222;line-height:1.75">
{goal_block}
    </div>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">The agents I&rsquo;ve watched do this well &mdash;
    they&rsquo;re not smarter, they&rsquo;re not grinding harder than everyone else.
    They just made a decision ahead of time about <em>when</em> they were going to pick up the phone.
    And then they showed up.</p>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">That&rsquo;s it.
    That&rsquo;s the whole thing.</p>

    <p style="margin:0 0 18px;font-size:16px;color:#222;line-height:1.75">Here&rsquo;s what I want you to do tonight.
    Click the link below. It takes three minutes.
    Pick the days. Pick the time. Pick how long.
    We&rsquo;ll send you a calendar invite and it goes on the team dashboard so I can see who&rsquo;s locked in.</p>

    <!-- CTA Button -->
    <table cellpadding="0" cellspacing="0" style="margin:32px 0">
      <tr><td style="background:#2563eb;border-radius:6px">
        <a href="{block_url}" style="display:inline-block;padding:16px 36px;font-family:Arial,sans-serif;font-size:15px;font-weight:700;color:#ffffff;text-decoration:none;letter-spacing:.02em">
          Set My Time Block &rarr;
        </a>
      </td></tr>
    </table>

    <p style="margin:0 0 10px;font-size:16px;color:#222;line-height:1.75">You didn&rsquo;t come this far to coast.</p>

    <p style="margin:0 0 32px;font-size:16px;color:#222;line-height:1.75">&mdash; Barry</p>

    <p style="margin:0;font-size:14px;color:#555;line-height:1.6;border-top:1px solid #eee;padding-top:20px">
      <em>P.S. Once it&rsquo;s set, I can see it on the team dashboard.
      There&rsquo;s nothing I want to see more this week than every agent on this team with their block locked in.</em>
    </p>
  </td></tr>

  <!-- Footer -->
  <tr><td style="background:#f8f8f8;padding:20px 36px;border-top:1px solid #eee">
    <p style="margin:0;font-size:11px;color:#999;font-family:Arial,sans-serif;line-height:1.6;text-align:center">
      Legacy Home Team &bull; Barry Jenkins &bull; Virginia&rsquo;s #1 Real Estate Team<br>
      <a href="__UNSUB_URL__" style="color:#999;text-decoration:underline">Unsubscribe</a>
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    plain = f"""{first_name},

I was looking at the team schedule this week.

Nobody's got their prospecting block set yet.

I'm not saying that to call anyone out — I'm saying it because I've watched this pattern play out long enough to know: that one thing is what separates the agents who hit their number from the ones who get to November wondering where the year went.

{"Working backwards from your goal, your model says you need roughly " + str(int(_db.compute_targets(goal).get("convos_needed_wk", 0))) + " real conversations a week to stay on track. That math doesn't happen by accident. It happens by appointment — with yourself." if goal and goal.get("gci_goal") else "The conversations that build your year don't just happen. You have to protect the time."}

The agents I've watched do this well — they just made a decision ahead of time about when they were going to pick up the phone. And then they showed up.

Here's what I want you to do tonight. Click the link below. It takes three minutes. Pick the days, pick the time, pick how long.

Set your time block: {block_url}

You didn't come this far to coast.

— Barry

P.S. Once it's set, I can see it on the team dashboard. There's nothing I want to see more this week than every agent on this team with their block locked in."""

    return subject, html, plain


@app.route("/api/goals/send-time-block-nudge", methods=["POST"])
def api_send_time_block_nudge():
    """
    Send a personalized time-block nudge email to every active agent
    who has NOT yet set a prospecting block, using their goal data for motivation.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    dry_run = (request.json or {}).get("dry_run", False)
    base_url = os.environ.get("BASE_URL", "").rstrip("/")
    if not base_url:
        base_url = request.host_url.rstrip("/")

    try:
        from pond_mailer import send_email

        profiles   = {p["agent_name"]: p for p in _db.get_agent_profiles(active_only=True)}
        all_goals  = {g["agent_name"]: g for g in _db.get_all_goals()}
        all_blocks = {b["agent_name"]: b for b in _db.get_all_prospecting_blocks()}

        results = []
        skipped = []

        for agent_name, profile in sorted(profiles.items()):
            email = profile.get("email", "").strip()
            if not email:
                skipped.append({"agent": agent_name, "reason": "no email on file"})
                continue

            # Only send to agents with NO block set (or block with 0 days)
            block = all_blocks.get(agent_name)
            if block and block.get("prospecting_days") and len(block["prospecting_days"]) > 0:
                skipped.append({"agent": agent_name, "reason": "block already set"})
                continue

            token = _db.get_token_for_agent(agent_name)
            if not token:
                token = _db.create_goal_token(agent_name)
            if not token:
                skipped.append({"agent": agent_name, "reason": "no token — run generate links first"})
                continue

            block_url = f"{base_url}/my-block/{token}"
            goal = all_goals.get(agent_name)
            first_name = agent_name.split()[0]

            subject, html_body, plain_body = _build_time_block_email_html(first_name, block_url, goal)

            try:
                result = send_email(email, subject, plain_body, html_body, dry_run=dry_run)
                results.append({
                    "agent":   agent_name,
                    "email":   email,
                    "subject": subject,
                    "status":  result.get("status"),
                    "has_goal": bool(goal and goal.get("gci_goal")),
                })
                logger.info("[TIME BLOCK NUDGE] Sent to %s <%s> — dry_run=%s", agent_name, email, dry_run)
            except Exception as _se:
                results.append({"agent": agent_name, "email": email, "status": "error", "error": str(_se)})

        return jsonify({
            "ok":      True,
            "sent":    len([r for r in results if r.get("status") in ("sent", "dry_run")]),
            "skipped": len(skipped),
            "dry_run": dry_run,
            "results": results,
            "skipped_detail": skipped,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ---- Generate setup links for agents ----

@app.route("/api/goals/generate-links", methods=["POST"])
def api_goals_generate_links():
    """
    Generate (or refresh) goal setup tokens for all active agents.
    Returns list of {agent_name, email, setup_url}.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    # Prefer explicit BASE_URL env var (required on Railway where the internal
    # host differs from the public URL). Falls back to the request host.
    base_url = (
        os.environ.get("BASE_URL", "").rstrip("/")
        or request.host_url.rstrip("/")
    )
    profiles = _db.get_agent_profiles(active_only=True)
    if not profiles:
        return jsonify({"error": "No agents found — run Scan Agents first"}), 404

    links = []
    for p in profiles:
        # Reuse existing valid token — don't rotate on every panel open (that breaks sent links)
        token = _db.get_token_for_agent(p["agent_name"]) or _db.create_goal_token(p["agent_name"])
        if token:
            links.append({
                "agent_name":   p["agent_name"],
                "email":        p["email"],
                "setup_url":    f"{base_url}/goals/setup/{token}",
                "my_goals_url": f"{base_url}/my-goals/{token}",
            })
    return jsonify({"success": True, "links": links})


# ---- Send goal setup emails ----

@app.route("/api/goals/send-emails", methods=["POST"])
def api_goals_send_emails():
    """
    Send personalised goal setup emails to agents.
    Body: { agents: [{agent_name, email, setup_url}] }  — output from generate-links.
    Optional: { agents: [...], test_mode: true } to preview without sending.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    body = request.json or {}
    agents = body.get("agents", [])
    test_mode = body.get("test_mode", False)

    if not agents:
        return jsonify({"error": "No agents provided"}), 400

    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key and not test_mode:
        return jsonify({"error": "SENDGRID_API_KEY not set"}), 503

    sent = []
    failed = []
    year = datetime.now().year

    for a in agents:
        name          = a.get("agent_name", "")
        email         = a.get("email", "")
        setup_url     = a.get("setup_url", "")
        my_goals_url  = a.get("my_goals_url", setup_url.replace("/goals/setup/", "/my-goals/"))
        first         = name.split()[0] if name else "there"

        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f0;padding:32px 16px">
  <tr><td align="center">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:560px">

    <!-- Header bar -->
    <tr>
      <td style="background:#0d1117;border-radius:12px 12px 0 0;padding:24px 32px;text-align:center">
        <img src="https://web-production-3363cc.up.railway.app/static/logo-white.png"
             alt="Legacy Home Team" width="160" style="display:block;margin:0 auto 10px;width:160px;height:auto">
        <p style="margin:0;font-size:20px;font-weight:800;color:#ffffff">{year} Income Goal Setting</p>
      </td>
    </tr>

    <!-- Body -->
    <tr>
      <td style="background:#ffffff;padding:36px 32px 28px;border-left:1px solid #e5e5e5;border-right:1px solid #e5e5e5">

        <p style="margin:0 0 20px;font-size:16px;color:#111111">Hey {first},</p>

        <p style="margin:0 0 16px;font-size:15px;line-height:1.65;color:#333333">
          I have one question for you:
          <strong style="color:#111111">What does your best year in real estate look like?</strong>
        </p>

        <p style="margin:0 0 24px;font-size:15px;line-height:1.65;color:#333333">
          Not a vague "I want to do better" answer — an actual number.
          Because the agents who hit big years aren't the ones with the most talent.
          They're the ones who decided, in January, exactly what they were going after.
        </p>

        <!-- Quote block -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 24px">
          <tr>
            <td style="border-left:4px solid #f5a623;padding:14px 20px;background:#fffbf0;border-radius:0 8px 8px 0">
              <p style="margin:0;font-size:15px;font-style:italic;color:#555555;line-height:1.6">
                "A goal properly set is halfway reached."
              </p>
              <p style="margin:8px 0 0;font-size:12px;font-weight:700;color:#f5a623;letter-spacing:0.5px">— ZIG ZIGLAR</p>
            </td>
          </tr>
        </table>

        <p style="margin:0 0 16px;font-size:15px;line-height:1.65;color:#333333">
          Here's what happens when you take 2 minutes to set your goal:
        </p>

        <!-- Benefit bullets -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 28px">
          <tr>
            <td style="padding:8px 0;font-size:14px;color:#333333;line-height:1.5">
              <span style="color:#f5a623;font-weight:800;margin-right:10px">✓</span>
              We calculate the exact number of calls and appointments you need — weekly, not yearly, so it never feels overwhelming.
            </td>
          </tr>
          <tr>
            <td style="padding:8px 0;font-size:14px;color:#333333;line-height:1.5">
              <span style="color:#f5a623;font-weight:800;margin-right:10px">✓</span>
              Your personal scorecard updates in real time — you can see your pace to goal every week, not just at year-end.
            </td>
          </tr>
          <tr>
            <td style="padding:8px 0;font-size:14px;color:#333333;line-height:1.5">
              <span style="color:#f5a623;font-weight:800;margin-right:10px">✓</span>
              I use your goal to prioritize lead routing, coaching conversations, and the support I put behind you this year.
            </td>
          </tr>
          <tr>
            <td style="padding:8px 0;font-size:14px;color:#333333;line-height:1.5">
              <span style="color:#f5a623;font-weight:800;margin-right:10px">✓</span>
              The agents on this team who set goals are the ones who actually hit them — and the ones I go to bat for when opportunities come up.
            </td>
          </tr>
        </table>

        <p style="margin:0 0 28px;font-size:15px;line-height:1.65;color:#333333">
          This link is yours alone. It takes about 2 minutes — just your income goal, commission rate, and average sale price.
          The math does the rest.
        </p>

        <!-- CTA button -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 28px">
          <tr>
            <td align="center">
              <a href="{setup_url}"
                 style="display:inline-block;background:#f5a623;color:#0d1117;padding:16px 36px;
                        border-radius:8px;text-decoration:none;font-weight:800;font-size:16px;
                        letter-spacing:0.3px">
                Set My {year} Income Goal →
              </a>
            </td>
          </tr>
        </table>

        <p style="margin:0 0 16px;font-size:13px;color:#888888;text-align:center">
          After setup, bookmark your personal dashboard:<br>
          <a href="{my_goals_url}" style="color:#f5a623;font-weight:700">View My Dashboard →</a>
        </p>

        <p style="margin:0 0 8px;font-size:15px;line-height:1.65;color:#333333">
          {first}, the sooner you set this, the sooner the system starts working for you.
          Don't leave this in your inbox — it'll take less time than a coffee run.
        </p>

        <p style="margin:24px 0 0;font-size:15px;color:#111111">
          Let's make {year} your best year yet,<br>
          <strong>Barry Jenkins</strong><br>
          <span style="font-size:13px;color:#888888">Legacy Home Team</span>
        </p>

      </td>
    </tr>

    <!-- Footer -->
    <tr>
      <td style="background:#f4f4f0;border:1px solid #e5e5e5;border-top:none;border-radius:0 0 12px 12px;padding:16px 32px;text-align:center">
        <p style="margin:0;font-size:12px;color:#999999;line-height:1.5">
          This link is personal to you — please don't share it. Works on your phone.<br>
          Legacy Home Team &middot; Charlotte, NC
        </p>
      </td>
    </tr>

  </table>
  </td></tr>
</table>
</body>
</html>"""

        plain_body = f"""Hey {first},

I have one question for you: what does your best year in real estate look like?

Not a vague "I want to do better" — an actual number. Because the agents who hit big years aren't the ones with the most talent. They're the ones who decided in January exactly what they were going after.

"A goal properly set is halfway reached." — Zig Ziglar

Here's what happens when you take 2 minutes to set your goal:

✓ We calculate the exact calls and appointments you need — weekly, so it never feels overwhelming.
✓ Your personal scorecard updates in real time so you can see your pace every week.
✓ I use your goal to prioritize lead routing, coaching, and the support I put behind you this year.
✓ The agents who set goals are the ones who hit them — and the ones I go to bat for.

Your personal setup link (takes 2 minutes, works on your phone):
{setup_url}

After you set up your goals, bookmark your personal dashboard here:
{my_goals_url}

{first}, the sooner you set this, the sooner the system starts working for you.

Let's make {year} your best year yet,
Barry Jenkins
Legacy Home Team

---
This link is personal to you — please don't share it.
"""

        if test_mode:
            sent.append({"agent_name": name, "email": email, "status": "preview",
                         "html": html_body})
            continue

        try:
            import sendgrid as _sg
            from sendgrid.helpers.mail import Mail as _Mail, Email as _Email
            sg = _sg.SendGridAPIClient(sg_key)
            msg = _Mail(
                from_email=config.EMAIL_FROM,
                to_emails=email,
                subject=f"{first}, what's your {year} income goal? (2 min)",
                html_content=html_body,
                plain_text_content=plain_body,
            )
            # CC Barry while monitoring agent onboarding
            msg.personalizations[0].add_cc(_Email(config.EMAIL_FROM))
            resp = sg.send(msg)
            sent.append({"agent_name": name, "email": email,
                         "status": "sent", "code": resp.status_code})
            print(f"[GOAL EMAIL] Sent to {name} <{email}> — HTTP {resp.status_code}")
        except Exception as e:
            failed.append({"agent_name": name, "email": email, "error": str(e)})
            print(f"[GOAL EMAIL] FAILED for {name} <{email}>: {e}")

    return jsonify({"success": True, "sent": sent, "failed": failed,
                    "test_mode": test_mode})


@app.route("/api/goals/announce-time-block", methods=["POST"])
def api_goals_announce_time_block():
    """
    Send the prospecting time-block feature rollout announcement to all active
    agents who have a goal on file. One-time blast — Barry triggers manually.

    Body: { test_mode: false }  — set test_mode:true to preview without sending.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    body      = request.json or {}
    test_mode = body.get("test_mode", False)

    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key and not test_mode:
        return jsonify({"error": "SENDGRID_API_KEY not set"}), 503

    base_url  = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")
    profiles  = _db.get_agent_profiles(active_only=True)
    year      = datetime.now().year
    sent      = []
    failed    = []

    for p in profiles:
        name      = p.get("agent_name", "")
        email     = p.get("email", "")
        token     = _db.get_goal_token(name) or ""
        if not email or not token:
            continue

        first       = name.split()[0]
        setup_url   = f"{base_url}/goals/setup/{token}"
        dash_url    = f"{base_url}/my-goals/{token}"
        # Check if they've already set their block
        pb          = _db.get_prospecting_block(name)
        has_block   = bool(pb and pb.get("prospecting_days"))

        cta_line = (
            "✅ You've already set your prospecting block — nice work. "
            "Head to your dashboard to see your schedule or update it any time."
            if has_block else
            "👉 Open your goal dashboard now to set your prospecting block — it takes 30 seconds."
        )
        cta_url   = dash_url if has_block else setup_url
        cta_label = "View My Dashboard →" if has_block else "Set My Time Block →"

        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#080c14;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<div style="max-width:560px;margin:0 auto">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#0d1117,#131d30);padding:28px 32px;
              text-align:center;border-radius:12px 12px 0 0">
    <img src="https://web-production-3363cc.up.railway.app/static/logo-white.png"
         alt="Legacy Home Team" width="150" style="display:block;margin:0 auto 10px;height:auto">
    <div style="font-size:11px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;
                color:#f5a623;margin-bottom:6px">New Feature Drop</div>
    <div style="font-size:22px;font-weight:800;color:#ffffff;line-height:1.3">
      📆 Prospecting Time Block is here, {first}
    </div>
  </div>

  <!-- Body -->
  <div style="background:#0f1520;padding:32px;border-left:1px solid #243050;border-right:1px solid #243050">

    <p style="margin:0 0 18px;font-size:15px;line-height:1.7;color:#e8edf8">
      Hey {first} — quick one.
    </p>

    <p style="margin:0 0 18px;font-size:15px;line-height:1.7;color:#e8edf8">
      I've been watching the data all year, and the single biggest difference between agents
      who hit their goals and agents who don't isn't skill, market, or even leads.
      <strong style="color:#f5a623">It's scheduled prospecting time.</strong>
    </p>

    <p style="margin:0 0 18px;font-size:15px;line-height:1.7;color:#e8edf8">
      The agents who block their calendar and protect it — even just 1 hour a day —
      compound that into deals. The ones who "do it when they can" run out of year.
    </p>

    <!-- Feature box -->
    <div style="background:linear-gradient(135deg,rgba(245,166,35,.1),rgba(245,166,35,.03));
                border:1px solid rgba(245,166,35,.3);border-radius:10px;padding:20px;margin:0 0 22px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
                  color:#f5a623;margin-bottom:8px">What's new in your dashboard</div>
      <div style="color:#e8edf8;font-size:14px;line-height:1.8">
        ✅ Pick the days you'll prospect (Mon–Sat)<br>
        ✅ Set your start time<br>
        ✅ Choose your block length (30 min to 2 hours)<br>
        ✅ Get a <strong>recurring calendar invite</strong> sent right to your email<br>
        ✅ Block shows on your dashboard — visible to both you and me
      </div>
    </div>

    <p style="margin:0 0 18px;font-size:15px;line-height:1.7;color:#e8edf8">
      {cta_line}
    </p>

    <!-- CTA button -->
    <div style="text-align:center;margin:0 0 22px">
      <a href="{cta_url}"
         style="display:inline-block;background:#f5a623;color:#080c14;font-weight:800;
                font-size:15px;padding:16px 36px;border-radius:8px;text-decoration:none;
                letter-spacing:0.3px">
        {cta_label}
      </a>
    </div>

    <p style="margin:0;font-size:15px;line-height:1.7;color:#e8edf8">
      This is one of the highest-leverage moves you can make right now.
      Takes 30 seconds. Pays off every week.<br><br>
      Let's go,<br>
      <strong style="color:#f5a623">Barry Jenkins</strong><br>
      <span style="font-size:13px;color:#68789a">Legacy Home Team</span>
    </p>
  </div>

  <!-- Footer -->
  <div style="background:#080c14;border:1px solid #243050;border-top:none;
              border-radius:0 0 12px 12px;padding:14px 32px;text-align:center">
    <p style="margin:0;font-size:11px;color:#3d506e;line-height:1.5">
      Legacy Home Team &middot; LPT Realty &middot; (757) 919-8874<br>
      <a href="{dash_url}" style="color:#3d506e">View Dashboard</a>
    </p>
  </div>

</div>
</body>
</html>"""

        plain_body = f"""Hey {first},

Quick one.

The single biggest difference between agents who hit their goals and those who don't isn't skill, market, or leads. It's scheduled prospecting time.

I just added a new feature to your goal dashboard: Prospecting Time Block.

Here's how it works:
• Pick the days you'll prospect (Mon-Sat)
• Set your start time
• Choose your block length (30 min to 2 hours)
• Get a recurring calendar invite sent to your email
• Block shows on your dashboard

{cta_line}

Link: {cta_url}

This takes 30 seconds and pays off every week.

Let's go,
Barry Jenkins
Legacy Home Team
"""

        subject = f"📆 New: Lock your prospecting time into your calendar, {first}"

        if test_mode:
            sent.append({"agent_name": name, "email": email, "status": "preview",
                         "has_block": has_block, "html_preview": html_body[:200]})
            continue

        try:
            import sendgrid as _sg
            from sendgrid.helpers.mail import Mail as _Mail, Email as _Email
            sg  = _sg.SendGridAPIClient(sg_key)
            msg = _Mail(
                from_email=config.EMAIL_FROM,
                to_emails=email,
                subject=subject,
                html_content=html_body,
                plain_text_content=plain_body,
            )
            resp = sg.send(msg)
            sent.append({"agent_name": name, "email": email,
                         "status": "sent", "code": resp.status_code, "has_block": has_block})
            print(f"[TIME BLOCK ANNOUNCE] Sent to {name} <{email}> — HTTP {resp.status_code}")
        except Exception as e:
            failed.append({"agent_name": name, "email": email, "error": str(e)})
            print(f"[TIME BLOCK ANNOUNCE] FAILED for {name} <{email}>: {e}")

    return jsonify({"success": True, "sent": sent, "failed": failed,
                    "test_mode": test_mode,
                    "total_agents": len(profiles)})


# ---- FUB Deal sync ----

@app.route("/api/goals/sync-deals", methods=["POST"])
def api_goals_sync_deals():
    """
    Pull deals from FUB (populated by Dotloop) and upsert into deal_log.
    Classifies each deal as 'contract' or 'closing' based on stage label.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    body = request.json or {}
    days_back = int(body.get("days_back", 365))

    try:
        client = FUBClient()
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        since = _dt.now(_tz.utc) - _td(days=days_back)
        deals = client.get_deals(since=since)
    except Exception as e:
        return jsonify({"error": f"FUB fetch failed: {e}"}), 500

    # Build name→commission_pct lookup from goals
    all_goals = _db.get_all_goals()
    comm_lookup = {g["agent_name"]: float(g["commission_pct"]) for g in all_goals}

    synced = 0
    skipped = 0
    unrecognised_stages = set()

    for deal in deals:
        stage_raw = (
            deal.get("stage") or deal.get("stageName") or
            deal.get("stageLabel") or ""
        )
        fub_deal_id = deal.get("id")
        if not fub_deal_id:
            continue

        # Agent name — FUB may store as assignedTo string or nested user object
        agent_raw = deal.get("assignedTo") or ""
        if isinstance(agent_raw, dict):
            agent_raw = (f"{agent_raw.get('firstName','')} "
                         f"{agent_raw.get('lastName','')}").strip()

        sale_price = deal.get("price") or deal.get("salePrice") or 0
        deal_name  = deal.get("name") or deal.get("address") or f"Deal #{fub_deal_id}"

        # Dates
        def _parse_date(val):
            if not val:
                return None
            try:
                from datetime import date as _d
                return _d.fromisoformat(str(val)[:10])
            except Exception:
                return None

        close_date    = _parse_date(deal.get("closedAt") or deal.get("closeDate"))
        contract_date = _parse_date(deal.get("contractDate") or deal.get("created"))

        comm_pct = comm_lookup.get(agent_raw)

        result = _db.upsert_deal(
            fub_deal_id=fub_deal_id,
            agent_name=agent_raw,
            deal_name=deal_name,
            sale_price=sale_price,
            stage_raw=stage_raw,
            contract_date=contract_date,
            close_date=close_date,
            commission_pct=comm_pct,
        )
        if result:
            synced += 1
        else:
            skipped += 1
            if stage_raw:
                unrecognised_stages.add(stage_raw)

    return jsonify({
        "success": True,
        "total_deals": len(deals),
        "synced": synced,
        "skipped_unrecognised": skipped,
        "unrecognised_stages": list(unrecognised_stages),
    })


# ---- Manual closing log ----

@app.route("/api/goals/send-nudge", methods=["POST"])
def api_goals_send_nudge():
    """
    Manager sends a custom Twilio SMS to one agent, with their why pre-populated.
    Body: {agent_name, message?, nudge_type?}
    """
    body = request.json or {}
    agent_name = body.get("agent_name")
    if not agent_name:
        return jsonify({"error": "agent_name required"}), 400

    profiles = {p["agent_name"]: p for p in _db.get_agent_profiles(active_only=False)}
    profile  = profiles.get(agent_name, {})
    email    = profile.get("email") or body.get("email")
    if not email:
        return jsonify({"error": f"No email address on file for {agent_name}"}), 400

    try:
        import nudge_engine as _nudge
        custom_msg = body.get("message")
        nudge_type = body.get("nudge_type", "custom")
        ok = _nudge.nudge_agent(
            agent_name, nudge_type, email,
            extra={"message": custom_msg} if custom_msg else None,
        )
        return jsonify({"success": ok})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/goals/nudge-context/<agent_name>")
def api_goals_nudge_context(agent_name):
    """Return agent why + identity for pre-populating the manager nudge compose box."""
    why   = _db.get_agent_why(agent_name) or {}
    ident = _db.get_agent_identity(agent_name) or {}
    streak= _db.get_streak(agent_name) or {}
    first = agent_name.split()[0]

    # Build a suggested message
    import nudge_engine as _nudge
    ctx = _nudge._ctx(agent_name)
    suggested = ""
    if why.get("why_statement"):
        suggested = (
            f"Hey {first}, checking in. Remember why you started: "
            f"\"{why['why_statement'][:80]}{'…' if len(why.get('why_statement','')) > 80 else ''}\" "
            f"Let's make today count."
        )
    else:
        suggested = f"Hey {first}, just wanted to check in and see how you're doing. What do you need from me this week?"

    return jsonify({
        "why": why,
        "identity": ident,
        "streak": streak,
        "suggested_message": suggested,
    })


@app.route("/api/admin/trigger-morning-nudges", methods=["POST"])
def api_trigger_morning_nudges():
    """One-time manual trigger for morning nudges (e.g. after a crash recovery).
    On Sunday, backfills Mon–Sat activity from FUB first so weekly totals are accurate."""
    try:
        from datetime import date as _date
        import nudge_engine as _nudge
        synced_days = 0
        if _date.today().weekday() == 6:   # Sunday
            sync_week_activity_from_fub()
            synced_days = _date.today().weekday()  # 6 days (Mon–Sat)
        sent_morning = _nudge.run_morning_nudges()
        sent_closing = _nudge.run_closing_milestones()
        return jsonify({"success": True, "morning_nudges": sent_morning,
                        "closing_milestones": sent_closing, "week_days_synced": synced_days})
    except Exception as e:
        logger.error("Manual nudge trigger failed: %s", e)
        return jsonify({"error": str(e)}), 500



@app.route("/api/admin/nudge-log-today")
def api_nudge_log_today():
    """Diagnostic: show today's nudge log entries with status."""
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, nudge_type, status,
                           sent_at AT TIME ZONE 'America/New_York' AS sent_et,
                           LEFT(message_content, 80) AS subject_preview
                    FROM nudge_log
                    WHERE sent_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY sent_at DESC
                """)
                rows = cur.fetchall()
        return jsonify([{
            "agent_name": r[0], "nudge_type": r[1], "status": r[2],
            "sent_et": str(r[3]), "subject_preview": r[4]
        } for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/nudge-audit")
def api_admin_nudge_audit():
    """Full readiness audit: agents, goals, yesterday's activity, tokens, nudge log."""
    from datetime import date, timedelta
    if not _db.is_available():
        return jsonify({"error": "DB not available"}), 503

    profiles  = _db.get_agent_profiles(active_only=True)
    all_goals = {g["agent_name"]: g for g in _db.get_all_goals(year=date.today().year)}
    team_data = _db.get_team_activity_yesterday()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    agents = []
    for p in profiles:
        name  = p["agent_name"]
        g     = all_goals.get(name, {})
        gci   = float(g.get("gci_goal", 0) or 0)
        act   = team_data.get(name, {})
        token = _db.get_goal_token(name) or ""
        counts = _db.get_nudge_counts_today(name)
        why   = _db.get_agent_why(name) or {}
        agents.append({
            "agent_name":    name,
            "email":         p.get("email") or "",
            "has_goal":      bool(gci),
            "gci_goal":      gci,
            "has_why":       bool(why.get("why_statement") or why.get("who_benefits")),
            "has_token":     bool(token),
            "yesterday_calls":  int(act.get("calls", 0) or 0),
            "yesterday_texts":  int(act.get("texts", 0) or 0),
            "yesterday_appts":  int(act.get("appts", 0) or 0),
            "nudge_sent_today": counts.get("morning", 0) > 0,
        })

    import os as _os
    return jsonify({
        "audit_date":      date.today().isoformat(),
        "yesterday":       yesterday,
        "agent_count":     len(agents),
        "goals_set":       sum(1 for a in agents if a["has_goal"]),
        "emails_present":  sum(1 for a in agents if a["email"]),
        "tokens_present":  sum(1 for a in agents if a["has_token"]),
        "nudge_sent_today":sum(1 for a in agents if a["nudge_sent_today"]),
        "sendgrid_key_present": bool(os.environ.get("SENDGRID_API_KEY")),
        "nudge_schedule":  "8:00am ET daily",
        "agents":          agents,
    })


@app.route("/api/admin/update-phones", methods=["POST"])
def api_admin_update_phones():
    """Batch-update agent phone numbers. Body: {phones: {agent_name: phone, ...}}"""
    if not _db.is_available():
        return jsonify({"error": "DB not available"}), 503
    phones = (request.json or {}).get("phones", {})
    results = {}
    for name, phone in phones.items():
        ok = _db.upsert_agent_profile(name, phone=phone.strip())
        results[name] = "ok" if ok else "failed"
    return jsonify({"success": True, "results": results})


@app.route("/api/admin/expire-isa-transfers", methods=["POST"])
def api_admin_expire_isa_transfers():
    """Manually trigger ISA_TRANSFER_FRESH expiry — same logic as the daily 6am job."""
    try:
        from config import ISA_TRANSFER_FRESH_TAG, ISA_TRANSFER_FRESH_DAYS
        expired = _db.get_expired_isa_transfers(days=ISA_TRANSFER_FRESH_DAYS)
        if not expired:
            return jsonify({"success": True, "removed": 0, "message": "No expired transfers"})
        api_key = os.environ.get("FUB_API_KEY", "")
        from fub_client import FUBClient
        client = FUBClient(api_key)
        removed, failed = 0, []
        for row in expired:
            person_id = row["person_id"]
            try:
                client.remove_tag(person_id, ISA_TRANSFER_FRESH_TAG)
                _db.delete_isa_transfer(person_id)
                removed += 1
            except Exception as e:
                failed.append({"person_id": person_id, "error": str(e)})
        return jsonify({"success": True, "removed": removed, "failed": failed})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/admin/update-emails", methods=["POST"])
def api_admin_update_emails():
    """Batch-update agent email addresses. Body: {emails: {agent_name: email, ...}}"""
    if not _db.is_available():
        return jsonify({"error": "DB not available"}), 503
    emails = (request.json or {}).get("emails", {})
    results = {}
    for name, email in emails.items():
        ok = _db.upsert_agent_profile(name, email=email.strip().lower())
        results[name] = "ok" if ok else "failed"
    return jsonify({"success": True, "results": results})


@app.route("/api/goals/sync-roster", methods=["POST"])
def api_sync_fub_roster():
    """Manually trigger a FUB roster sync. Detects new agents and sends onboarding emails."""
    try:
        result = sync_fub_roster()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/goals/sync-activity", methods=["POST"])
def api_sync_daily_activity():
    """Manually trigger FUB activity sync (yesterday + today) into daily_activity table."""
    try:
        _et_h = -4 if 3 <= datetime.now(timezone.utc).month <= 10 else -5
        ET_tz = timezone(timedelta(hours=_et_h))
        today_et     = datetime.now(ET_tz).date()
        yesterday_et = today_et - timedelta(days=1)
        sync_daily_activity_from_fub(target_date=yesterday_et)
        sync_daily_activity_from_fub(target_date=today_et)
        return jsonify({"ok": True, "message": "Activity sync complete (yesterday + today)"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/goals/sync-ytd-cache", methods=["POST"])
def api_sync_ytd_cache():
    """Rebuild the YTD calls + appointments cache for all agents from FUB."""
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503
    try:
        year = datetime.now(timezone.utc).year
        client = FUBClient()
        year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        ytd_calls = client.get_calls(since=year_start)
        ytd_appts = client.get_appointments(since=year_start)

        fub_users = {
            f"{u.get('firstName','')} {u.get('lastName','')}".strip(): u.get("id")
            for u in client.get_users()
        }
        _convo_thresh = config.CONVERSATION_THRESHOLD_SECONDS
        calls_by_uid  = {}
        convos_by_uid = {}
        for call in ytd_calls:
            if call.get("isIncoming"):
                continue
            uid = call.get("userId")
            if not uid:
                continue
            calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1
            dur = call.get("duration", 0) or 0
            if dur >= _convo_thresh:
                convos_by_uid[uid] = convos_by_uid.get(uid, 0) + 1

        appts_by_uid = {}
        for appt in ytd_appts:
            invitees = appt.get("invitees") or []
            has_lead = any(inv.get("personId") for inv in invitees)
            if not has_lead:
                continue
            for inv in invitees:
                uid = inv.get("userId")
                if uid:
                    appts_by_uid[uid] = appts_by_uid.get(uid, 0) + 1

        cached = 0
        details = {}
        for agent_name, fub_uid in fub_users.items():
            if agent_name in config.EXCLUDED_USERS:
                continue
            calls = calls_by_uid.get(fub_uid, 0)
            convos = convos_by_uid.get(fub_uid, 0)
            appts = appts_by_uid.get(fub_uid, 0)
            if _db.upsert_ytd_cache(agent_name, year, calls, appts, convos_ytd=convos):
                cached += 1
                details[agent_name] = {"calls_ytd": calls, "convos_ytd": convos, "appts_ytd": appts}

        return jsonify({"ok": True, "agents_updated": cached, "year": year, "details": details})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/goals/log-closing", methods=["POST"])
def api_goals_log_closing():
    """Manually log a closing. Body: {agent_name, deal_name, sale_price, close_date?}"""
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503
    body = request.json or {}
    required = ["agent_name", "deal_name", "sale_price"]
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Missing: {missing}"}), 400

    from datetime import date as _date
    close_date = None
    if body.get("close_date"):
        try:
            close_date = _date.fromisoformat(body["close_date"])
        except Exception:
            pass

    agent_name = body["agent_name"]
    goal = _db.get_goal(agent_name)
    comm_pct = float(goal["commission_pct"]) if goal else None

    ok = _db.log_manual_closing(
        agent_name=agent_name,
        deal_name=body["deal_name"],
        sale_price=float(body["sale_price"]),
        close_date=close_date,
        commission_pct=comm_pct,
    )

    # Notify Barry of first close milestone
    try:
        year = datetime.now(timezone.utc).year
        deals = _db.get_deal_summary(agent_name=agent_name, year=year)
        closings = deals.get("closings", 0)
        if closings == 1:  # First closing of the year
            api_key = os.environ.get("SENDGRID_API_KEY")
            if api_key:
                import sendgrid as _sg
                from sendgrid.helpers.mail import Mail as _Mail
                msg = _Mail(
                    from_email=config.EMAIL_FROM,
                    to_emails=config.EMAIL_FROM,
                    subject=f"🏆 {agent_name} just logged their first closing of {year}!",
                    plain_text_content=(
                        f"{agent_name} logged their first closing of {year}.\n\n"
                        f"Deal: {body.get('deal_name', 'Unknown')}\n"
                        f"Price: ${float(body.get('sale_price', 0)):,.0f}\n\n"
                        "This is a great moment to reach out personally."
                    ),
                )
                _sg.SendGridAPIClient(api_key).send(msg)
    except Exception:
        pass  # Non-fatal

    return jsonify({"success": ok})


# ---- Scorecard API (manager view) ----

@app.route("/api/goals/scorecard")
def api_goals_scorecard():
    """
    Return full scorecard for all agents: goals + targets + YTD actuals + pace.

    Reads entirely from DB (engagement cache + deal_log) — no live FUB calls.
    Data is refreshed automatically Mon + Thu 6am ET by the goals_sync job.
    Pass ?force_refresh=1 to trigger a live FUB pull right now (slow, admin use).
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected", "scorecard": []}), 503

    year = int(request.args.get("year", datetime.now().year))
    force_refresh = request.args.get("force_refresh") == "1"

    if force_refresh:
        # Run synchronously so the response contains the freshly-pulled data.
        # Takes ~30-60s — the frontend disables the button and shows "syncing…"
        # while waiting, so this is expected.
        scheduled_sync_goals_data()

    all_goals      = _db.get_all_goals(year=year)
    deal_summaries = _db.get_deal_summary(year=year)
    profiles       = {p["agent_name"]: p for p in _db.get_agent_profiles()}
    base_url       = os.environ.get("BASE_URL", "").rstrip("/")

    # Read from cache (populated by scheduled job or the force_refresh above)
    ytd_cache     = _db.get_ytd_cache(year=year)
    cache_updated = _db.get_cache_updated_at(year=year)

    # Build a complete list — agents WITH goals first, then agents with no goals.
    # This ensures every agent_profile is visible on the scorecard so Barry can
    # see who still needs to set goals.
    goals_map = {g["agent_name"]: g for g in all_goals}
    all_agents = sorted(set(goals_map.keys()) | set(profiles.keys()))

    _empty_pace = {
        "overall_status": "gray", "overall_pct": 0,
        "needs_conversation": False, "week_num": datetime.now().isocalendar()[1],
        "pct_of_year": 0, "calls": None, "appointments": None, "closings": None,
    }

    scorecard = []
    for agent in all_agents:
        goal    = goals_map.get(agent)
        profile = profiles.get(agent, {})

        _token = _db.get_token_for_agent(agent)
        _my_goals_url = f"{base_url}/my-goals/{_token}" if (base_url and _token) else ""

        if not goal:
            # Agent has a profile but hasn't set their goals yet
            scorecard.append({
                "agent_name":   agent,
                "email":        profile.get("email", ""),
                "goal":         None,
                "targets":      {},
                "actuals":      {"calls_ytd": 0, "convos_ytd": 0, "appts_ytd": 0,
                                 "contracts_ytd": 0, "closings_ytd": 0, "gci_ytd": 0},
                "pace":         _empty_pace,
                "my_goals_url": _my_goals_url,
            })
            continue

        targets = _db.compute_targets(goal)
        deals   = deal_summaries.get(agent, {"contracts": 0, "closings": 0, "gci_est": 0.0})
        cached  = ytd_cache.get(agent, {"calls_ytd": 0, "appts_ytd": 0, "convos_ytd": 0})
        actuals = {
            "calls_ytd":     cached["calls_ytd"],
            "convos_ytd":    cached.get("convos_ytd", 0),
            "appts_ytd":     cached["appts_ytd"],
            "contracts_ytd": deals["contracts"],
            "closings_ytd":  deals["closings"],
            "gci_ytd":       deals["gci_est"],
        }
        _start_date = profile.get("start_date")
        pace = _db.compute_pace(goal, targets, actuals, start_date=_start_date)
        scorecard.append({
            "agent_name":   agent,
            "email":        profile.get("email", ""),
            "start_date":   _start_date,
            "goal":         goal,
            "targets":      targets,
            "actuals":      actuals,
            "pace":         pace,
            "my_goals_url": _my_goals_url,
        })

    # Sort: red → yellow → green → no-goal (gray)
    _order = {"red": 0, "yellow": 1, "green": 2, "gray": 3}
    scorecard.sort(key=lambda x: _order.get(x["pace"]["overall_status"], 3))

    # Attach streak data so manager cards can show 🔥 streak + last active
    all_streaks = _db.get_all_streaks()

    return jsonify({
        "scorecard":     scorecard,
        "streaks":       all_streaks,
        "year":          year,
        "week_num":      datetime.now().isocalendar()[1],
        "cache_updated": cache_updated,
        "force_refresh": force_refresh,
    })


@app.route("/api/goals/agents-list")
def api_goals_agents_list():
    """Lightweight list of active agent names + emails + start dates for dropdowns."""
    profiles = _db.get_agent_profiles(active_only=True)
    return jsonify({
        "agents": [{"name": p["agent_name"], "email": p.get("email", ""),
                    "start_date": p.get("start_date")} for p in profiles]
    })


@app.route("/api/goals/set-start-date", methods=["POST"])
def api_set_agent_start_date():
    """Barry sets an agent's team start date. Body: {agent_name, start_date (YYYY-MM-DD)}"""
    body = request.get_json(silent=True) or {}
    agent_name = body.get("agent_name", "").strip()
    start_date = body.get("start_date", "").strip()
    if not agent_name or not start_date:
        return jsonify({"ok": False, "error": "agent_name and start_date required"}), 400
    ok = _db.set_agent_start_date(agent_name, start_date)
    return jsonify({"ok": ok})


@app.route("/api/goals/meeting-brief/<path:agent_name>")
def api_meeting_brief(agent_name):
    """
    Generate Barry's AI coaching brief for a 1-on-1 meeting with an agent.
    Synthesizes goal pace, funnel breakdown, team rank, and agent 'why' into
    a structured coaching guide — not agent-facing, Barry only.
    """
    if not _db.is_available():
        return jsonify({"error": "Database not connected"}), 503

    try:
        import anthropic as _anthropic
        import json as _json

        year      = datetime.now().year
        week_num  = datetime.now().isocalendar()[1]
        first_name = agent_name.split()[0]

        # ── Gather agent data ─────────────────────────────────────────────
        goal         = _db.get_goal(agent_name, year=year)
        why          = _db.get_agent_why(agent_name) or {}
        streak       = _db.get_streak(agent_name) or {}
        profiles     = {p["agent_name"]: p for p in _db.get_agent_profiles(active_only=False)}
        start_date   = (profiles.get(agent_name) or {}).get("start_date")  # ISO str or None
        ytd_cache    = _db.get_ytd_cache(year=year)
        agent_ytd    = ytd_cache.get(agent_name, {})
        deal_summary = _db.get_deal_summary(agent_name, year=year)

        # If YTD cache is empty for this agent (goals sync hasn't run yet, or agent is new),
        # fall back to summing directly from the daily_activity table — same FUB data,
        # just a different aggregation path that's always current.
        _ytd_source = "cache"
        if not agent_ytd or (agent_ytd.get("calls_ytd", 0) == 0
                              and agent_ytd.get("convos_ytd", 0) == 0
                              and agent_ytd.get("appts_ytd", 0) == 0):
            agent_ytd = _db.get_ytd_from_daily_activity(agent_name, year=year)
            _ytd_source = "daily_activity"
            logger.info("meeting-brief: using daily_activity fallback for %s (ytd cache empty)", agent_name)
        all_deals    = _db.get_deal_summary(year=year)   # all agents for team rank

        targets  = _db.compute_targets(goal) if goal else {}
        actuals  = {
            "calls_ytd":     agent_ytd.get("calls_ytd", 0),
            "convos_ytd":    agent_ytd.get("convos_ytd", 0),
            "appts_ytd":     agent_ytd.get("appts_ytd", 0),
            "contracts_ytd": deal_summary.get("contracts", 0),
            "closings_ytd":  deal_summary.get("closings", 0),
            "gci_ytd":       deal_summary.get("gci_est", 0),
        }
        pace    = _db.compute_pace(goal, targets, actuals, start_date=start_date) if goal else {}
        act_ctx = _db.get_agent_activity_context(agent_name)

        # ── Team rank for each funnel metric ──────────────────────────────
        # Build a merged YTD snapshot: start with ytd_cache, then fill any
        # agent who's missing or all-zero from daily_activity (new agents,
        # agents whose cache hasn't synced yet).
        all_daily = _db.get_all_ytd_from_daily_activity(year=year)
        merged_ytd = {}
        all_profile_names = set(profiles.keys())
        for name in all_profile_names | set(ytd_cache.keys()) | set(all_daily.keys()):
            cached = ytd_cache.get(name, {})
            cached_empty = (not cached or
                            (cached.get("calls_ytd", 0) == 0
                             and cached.get("convos_ytd", 0) == 0
                             and cached.get("appts_ytd", 0) == 0))
            merged_ytd[name] = all_daily.get(name, cached) if cached_empty else cached

        def _rank(metric_key, ytd_key, source="ytd"):
            """Returns (rank, total, agent_val, team_avg)."""
            if source == "ytd":
                values = [(n, d.get(ytd_key, 0)) for n, d in merged_ytd.items()]
            else:
                values = [(n, all_deals.get(n, {}).get(ytd_key, 0)) for n in merged_ytd.keys()]
            values.sort(key=lambda x: x[1], reverse=True)
            total = max(len(values), 1)
            agent_val = next((v for n, v in values if n == agent_name), 0)
            team_avg  = sum(v for _, v in values) / total
            rank = next((i + 1 for i, (n, _) in enumerate(values) if n == agent_name), total)
            return rank, total, agent_val, round(team_avg, 1)

        convo_rank,  n_agents, _, team_convo_avg  = _rank("convos",   "convos_ytd")
        appt_rank,   _,        _, team_appt_avg   = _rank("appts",    "appts_ytd")
        closing_rank,_,        _, team_close_avg  = _rank("closings", "closings", source="deals")

        # ── Funnel bottleneck — lowest % of pace ──────────────────────────
        funnel_pcts = {
            "Conversations": (pace.get("convos",       {}) or {}).get("pct", 0),
            "Appointments":  (pace.get("appointments", {}) or {}).get("pct", 0),
            "Closings":      (pace.get("closings",     {}) or {}).get("pct", 0),
        }
        bottleneck = min(funnel_pcts, key=funnel_pcts.get)

        # ── Build AI prompt ───────────────────────────────────────────────
        gci_goal          = goal.get("gci_goal", 0) if goal else 0
        convos_per_week   = targets.get("convos_per_week", 0)
        appts_per_week    = targets.get("appts_per_week", 0)
        overall_pct       = pace.get("overall_pct", 0) if pace else 0
        overall_status    = pace.get("overall_status", "gray") if pace else "gray"
        convo_pct         = funnel_pcts["Conversations"]
        convo_target_ytd  = (pace.get("convos", {}) or {}).get("target_ytd", 0)
        appt_pct          = funnel_pcts["Appointments"]
        appt_target_ytd   = (pace.get("appointments", {}) or {}).get("target_ytd", 0)
        closing_pct       = funnel_pcts["Closings"]
        why_stmt  = why.get("why_statement", "") if why else ""
        who_ben   = why.get("who_benefits", "") if why else ""
        what_hap  = why.get("what_happens", "") if why else ""

        prompt = f"""You are helping Barry Jenkins prepare for a 1-on-1 coaching meeting with one of his agents.

Barry Jenkins background:
- Team leader of Legacy Home Team, Virginia's #1 real estate team (850+ homes/year)
- Author of "Too Nice for Sales" — his entire philosophy: teaching beats pushing, serve people genuinely
- Former pastor of 10 years. He holds the mirror up clearly but the reflection is always an opportunity.
- Core phrase: "You're being too nice to your comfort zone." He names avoidance with empathy, then redirects.
- He uses atomic habits / identity-based language: "You're the kind of agent who..." not "you need to do X"
- Never shame. Always reframe. Every miss is "a pitch you let go by — more are coming."
- Ends every coaching moment with one specific, concrete next step.
- Conversational voice. Short sentences. Real words. No corporate language. No motivational poster phrases.

This brief is for BARRY ONLY — the agent never sees it. Be honest and direct. Barry can handle the full picture.

━━━━ AGENT: {agent_name} (refer to them as {first_name}) ━━━━
Week {week_num} of 52  |  GCI Goal: ${gci_goal:,.0f}/year
{f"Team start date: {start_date} ({pace.get('weeks_on_team', 0)} weeks on the team — PRORATE all expectations accordingly. This is a new agent. Do NOT compare their YTD numbers to a full-year agent's pace." if pace.get('is_new_agent') else "Full-year agent — full annual targets apply."}
Overall Pace: {overall_pct}% of their {f"{pace.get('weeks_on_team', 0)}-week" if pace.get("is_new_agent") else "YTD"} target ({overall_status.upper()})

YTD Funnel (actual vs. target-by-now):
- Conversations (≥2 min calls): {actuals['convos_ytd']} actual / {convo_target_ytd} needed by now → {convo_pct}% of pace
- Dials made: {actuals['calls_ytd']} outbound calls
- Appointments set: {actuals['appts_ytd']} actual / {appt_target_ytd} needed by now → {appt_pct}% of pace
- Contracts: {actuals['contracts_ytd']}
- Closings: {actuals['closings_ytd']} → {closing_pct}% of pace
- GCI earned: ${actuals['gci_ytd']:,.0f}

Weekly targets: {convos_per_week} conversations/wk · {appts_per_week} appointments/wk
Primary bottleneck (weakest funnel step): {bottleneck} at {funnel_pcts[bottleneck]}% of pace

Recent momentum (last 60 days — use this to speak to current effort, not just YTD totals):
- This week so far:  {act_ctx.get('windows',{}).get('this_week',{}).get('convos',0)} convos · {act_ctx.get('windows',{}).get('this_week',{}).get('appts',0)} appts · {act_ctx.get('windows',{}).get('this_week',{}).get('calls',0)} dials
- Last full week:    {act_ctx.get('windows',{}).get('last_week',{}).get('convos',0)} convos · {act_ctx.get('windows',{}).get('last_week',{}).get('appts',0)} appts · {act_ctx.get('windows',{}).get('last_week',{}).get('calls',0)} dials
- This month (MTD): {act_ctx.get('windows',{}).get('mtd',{}).get('convos',0)} convos · {act_ctx.get('windows',{}).get('mtd',{}).get('appts',0)} appts
- Last month full:  {act_ctx.get('windows',{}).get('last_month',{}).get('convos',0)} convos · {act_ctx.get('windows',{}).get('last_month',{}).get('appts',0)} appts
- 60-day trend direction: {act_ctx.get('trend_dir','unknown')} (recent 3-week avg: {act_ctx.get('avg_recent',0)} convos/wk vs prior avg: {act_ctx.get('avg_prior',0)} convos/wk)

Team comparison ({n_agents} agents):
- Conversations: ranked #{convo_rank} of {n_agents} (team avg: {team_convo_avg})
- Appointments:  ranked #{appt_rank} of {n_agents} (team avg: {team_appt_avg})
- Closings:      ranked #{closing_rank} of {n_agents} (team avg: {team_close_avg})

Streak: {streak.get('current_streak', 0)} day active streak (best ever: {streak.get('longest_streak', streak.get('best_streak', 0))})

Agent's "Why" (Cheplak identity framework):
- Why statement: {why_stmt or "(not filled in — agent hasn't set this yet)"}
- Who benefits: {who_ben or "(not set)"}
- What happens for them: {what_hap or "(not set)"}

━━━━ GENERATE THE 1-ON-1 MEETING BRIEF ━━━━

Return a JSON object with exactly these 6 keys. Write Barry's voice throughout:

{{
  "situation": "2-3 sentences. Where {first_name} stands right now. Specific numbers, honest read, no spin. What the overall pace says and what the trend in the funnel suggests.",

  "bottleneck": "1-2 sentences. The single most important thing to address — not a general observation. Identify exactly where {first_name}'s funnel breaks down and what it's costing them in concrete terms (missed appointments, missed GCI, etc).",

  "talking_points": [
    {{
      "topic": "3-5 word label",
      "what_to_say": "2-3 sentences Barry says out loud. Conversational, specific to {first_name}'s numbers, teaching not pushing. Opens with a real observation or relatable situation before the lesson.",
      "question": "The coaching question Barry asks. Open-ended. Leads {first_name} to see the answer themselves — not yes/no."
    }},
    {{
      "topic": "3-5 word label",
      "what_to_say": "...",
      "question": "..."
    }},
    {{
      "topic": "Identity — Atomic Habits",
      "what_to_say": "The identity reframe. 'You're the kind of agent who...' — tie to who {first_name} is becoming, not just what they need to do differently. Include one tiny, specific habit change that makes the right behavior obvious and easy.",
      "question": "A question about self-image and identity — not behavior."
    }}
  ],

  "why_connection": "2-3 sentences. Connect {first_name}'s current performance gap to their why. Practical, not preachy. If the why is blank, write about how finding that reason is the first step — and what Barry should ask to surface it.",

  "team_comparison": "2-3 sentences. {first_name}'s position on the team, framed as a gap to close — not a ranking to shame. Make it motivating. What does closing that gap look like in concrete weekly actions?",

  "commitment": "The single specific commitment {first_name} makes before leaving this meeting. One number, one behavior, one date. Concrete enough that Barry texts them about it next week."
}}

Write in Barry's voice. Contractions. Short sentences. No 'feel free to', 'I'd love to', 'don't hesitate'. No bullet points inside the text. Just Barry talking."""

        ai_client = _anthropic.Anthropic()
        msg = ai_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1800,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()
        brief = _json.loads(raw)

        team_rank = {
            "convos":   {"rank": convo_rank,   "total": n_agents, "team_avg": team_convo_avg},
            "appts":    {"rank": appt_rank,    "total": n_agents, "team_avg": team_appt_avg},
            "closings": {"rank": closing_rank, "total": n_agents, "team_avg": team_close_avg},
        }

        # Auto-save to DB — silently, never block the response
        try:
            _db.save_meeting_brief(
                agent_name=agent_name,
                week_num=week_num,
                year=datetime.now().year,
                brief=brief,
                actuals=actuals,
                pace=pace,
                meta={
                    "funnel_pcts": funnel_pcts,
                    "bottleneck":  bottleneck,
                    "team_rank":   team_rank,
                    "streak":      streak,
                    "first_name":  first_name,
                    "goal":        goal,
                    "targets":     targets,
                    "act_ctx":     act_ctx,
                },
            )
        except Exception as _save_err:
            logger.warning("meeting-brief save failed (non-fatal): %s", _save_err)

        return jsonify({
            "agent_name":   agent_name,
            "first_name":   first_name,
            "week_num":     week_num,
            "goal":         goal,
            "targets":      targets,
            "actuals":      actuals,
            "pace":         pace,
            "funnel_pcts":  funnel_pcts,
            "bottleneck":   bottleneck,
            "team_rank":    team_rank,
            "streak":       streak,
            "brief":        brief,
            "act_ctx":      act_ctx,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ytd_source":   _ytd_source,
        })

    except Exception as e:
        import traceback
        logger.error("meeting-brief error for %s: %s\n%s", agent_name, e, traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/goals/meeting-history/<path:agent_name>")
def api_meeting_history(agent_name):
    """Return past meeting briefs for an agent, newest first."""
    briefs = _db.get_meeting_briefs(agent_name, limit=20)
    return jsonify({"agent_name": agent_name, "briefs": briefs})


@app.route("/api/goals/meeting-brief-record/<int:brief_id>", methods=["DELETE"])
def api_delete_meeting_brief(brief_id):
    """Delete a saved meeting brief by id."""
    ok = _db.delete_meeting_brief(brief_id)
    if ok:
        return jsonify({"deleted": brief_id})
    return jsonify({"error": "Delete failed or record not found"}), 404


@app.route("/api/goals/scorecard-meta")
def api_goals_scorecard_meta():
    """
    Returns metadata for manager scorecard:
    - Agents with no goal setup
    - Agents with no phone number
    - Agents gone dark (no activity in 10+ days)
    """
    return jsonify({
        "no_setup":   _db.get_agents_no_goal_setup(),
        "no_phone":   _db.get_agents_no_phone(),
        "gone_dark":  _db.get_agents_gone_dark(days=10),
    })


# ---- LeadStream: Weekly Engagement Tracker ----

@app.route("/api/leadstream/weekly")
def api_leadstream_weekly():
    """Return 7-day per-agent engagement history. Reads from disk only — no FUB API calls."""
    import json as _json
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    _cache_base = (
        os.environ.get("LEADSTREAM_CACHE_DIR")
        or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
    )
    eng_log_path = os.path.join(_cache_base, "engagement_log.json")

    # Try DB first, then file fallback
    if _db.is_available():
        eng_log = _db.read_engagement_log(days=7)
    else:
        eng_log = None
        for _rp in [eng_log_path, "/tmp/.cache/engagement_log.json"]:
            try:
                with open(_rp) as f:
                    eng_log = _json.load(f)
                break
            except Exception:
                continue
    if not eng_log:
        return jsonify({"runs": [], "agents": [], "storage_path": eng_log_path,
                        "db_active": _db.is_available()})

    now = _dt.now(_tz.utc)
    cutoff = (now - _td(days=7)).isoformat()
    recent = {k: v for k, v in eng_log.items() if k >= cutoff}
    sorted_runs = sorted(recent.items(), key=lambda x: x[0])

    all_agents = set()
    for _, rec in sorted_runs:
        all_agents.update(rec.get("agents", {}).keys())
    all_agents = sorted(all_agents)

    # ET offset for label display (EDT Apr-Oct = -4, EST Nov-Mar = -5)
    _et_h = -4 if 3 <= now.month <= 10 else -5
    _ET = _td(hours=_et_h)

    # Aggregate multiple runs per day into one column per day.
    # tagged = max seen that day (same pool each run)
    # actioned = max seen that day (a lead actioned once stays actioned)
    days_map = {}  # "YYYY-MM-DD" → aggregated record
    days_order = []

    for run_time, rec in sorted_runs:
        try:
            dt_utc = _dt.fromisoformat(run_time.replace("Z", "+00:00"))
            dt_et = dt_utc + _ET
            day_key = dt_et.strftime("%Y-%m-%d")
            day_label = dt_et.strftime("%a %-m/%-d")
        except Exception:
            day_key = run_time[:10]
            day_label = run_time[:10]

        if day_key not in days_map:
            days_map[day_key] = {"label": day_label, "agents": {}, "mode": rec.get("mode", "full")}
            days_order.append(day_key)

        day = days_map[day_key]
        for agent in all_agents:
            a = rec.get("agents", {}).get(agent, {"tagged": 0, "actioned": 0})
            t = a.get("tagged", 0)
            ac = a.get("actioned", 0)
            prev = day["agents"].get(agent, {"tagged": 0, "actioned": 0})
            day["agents"][agent] = {
                "tagged": max(prev["tagged"], t),
                "actioned": max(prev["actioned"], ac),
            }

    runs_out = []
    for day_key in days_order:
        day = days_map[day_key]
        agents_data = {}
        for agent in all_agents:
            a = day["agents"].get(agent, {"tagged": 0, "actioned": 0})
            t = a["tagged"]
            ac = a["actioned"]
            agents_data[agent] = {
                "tagged": t,
                "actioned": ac,
                "rate": round(ac / t * 100) if t > 0 else 0,
            }
        total_tagged = max((a["tagged"] for a in agents_data.values()), default=0)
        total_actioned = sum(a["actioned"] for a in agents_data.values())
        runs_out.append({
            "run_time": day_key,
            "label": day["label"],
            "mode": day["mode"],
            "agents": agents_data,
            "total": {
                "tagged": total_tagged,
                "actioned": total_actioned,
                "rate": round(total_actioned / total_tagged * 100) if total_tagged > 0 else 0,
            },
        })

    return jsonify({"runs": runs_out, "agents": all_agents})


# ---- LeadStream: FUB Webhook (real-time tag removal) ----

@app.route("/webhook/fub", methods=["POST"])
def webhook_fub():
    """
    Receives FUB webhook events and immediately removes the LeadStream tag
    from any lead that gets an outbound call or text — no waiting for the
    next 4-hour scoring run.

    Configure in FUB: Admin → Integrations → Webhooks
      URL: https://<your-app>/webhook/fub
      Events: Call Log Created, Text Message Created
    """
    # Optional secret verification
    webhook_secret = os.environ.get("FUB_WEBHOOK_SECRET")
    if webhook_secret:
        provided = (
            request.headers.get("X-FUB-Signature")
            or request.headers.get("Authorization", "").replace("Bearer ", "")
        )
        if provided != webhook_secret:
            return jsonify({"error": "unauthorized"}), 401

    payload = request.json or {}

    # FUB wraps events as {"event": "...", "data": {...}}
    event = payload.get("event") or payload.get("type", "")
    event_data = payload.get("data") or payload

    person_id = event_data.get("personId")
    if not person_id:
        return jsonify({"ok": True, "action": "ignored_no_person"})

    # Determine if this is an outbound contact
    is_outbound = False
    if "call" in event.lower():
        is_outbound = not event_data.get("isIncoming", True)
    elif "text" in event.lower():
        is_outbound = event_data.get("isOutbound", False)

    if not is_outbound:
        return jsonify({"ok": True, "action": "ignored_inbound"})

    # Remove LeadStream tags immediately
    try:
        from config import LEADSTREAM_TAG, LEADSTREAM_POND_TAG
        client = FUBClient()
        person = client.get_person(person_id)
        tags = person.get("tags") or []

        tags_to_remove = {LEADSTREAM_TAG, LEADSTREAM_POND_TAG}
        removed = [t for t in tags if t in tags_to_remove]

        if removed:
            new_tags = [t for t in tags if t not in tags_to_remove]
            client._request("PUT", f"people/{person_id}", json_data={"tags": new_tags})

        return jsonify({
            "ok": True,
            "personId": person_id,
            "action": "tags_removed" if removed else "no_leadstream_tag",
            "removed": removed,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---- BatchLeads → FUB Webhook (via Zapier) ----

@app.route("/webhook/batchleads", methods=["POST"])
def webhook_batchleads():
    """
    Receives property/lead data from BatchLeads via Zapier and creates
    a contact in Follow Up Boss with deduplication.

    Zapier setup:
      Trigger: BatchLeads → New Property Added / Property Skip Traced
      Action:  Webhooks by Zapier → POST to https://<your-app>/webhook/batchleads

    Accepts flexible field names to handle various Zapier mappings.
    """
    # Optional secret verification
    webhook_secret = os.environ.get("BATCHLEADS_WEBHOOK_SECRET")
    if webhook_secret:
        provided = (
            request.headers.get("X-Webhook-Secret")
            or request.headers.get("Authorization", "").replace("Bearer ", "")
        )
        if provided != webhook_secret:
            return jsonify({"error": "unauthorized"}), 401

    payload = request.json or {}

    if not payload:
        return jsonify({"error": "empty payload"}), 400

    # Handle both single lead and batch (array) payloads
    leads = payload if isinstance(payload, list) else [payload]
    results = []

    client = FUBClient()

    for lead in leads:
        try:
            result = _process_batchleads_lead(client, lead)
            results.append(result)
        except Exception as e:
            results.append({"error": str(e), "lead": lead.get("address", "unknown")})

    return jsonify({
        "ok": True,
        "processed": len(results),
        "results": results,
    })


def _process_batchleads_lead(client, lead):
    """Map BatchLeads/Zapier fields to FUB and create person."""

    # Extract owner name — BatchLeads uses various field names
    first_name = (
        lead.get("owner_first_name")
        or lead.get("first_name")
        or lead.get("firstName")
        or ""
    ).strip()
    last_name = (
        lead.get("owner_last_name")
        or lead.get("last_name")
        or lead.get("lastName")
        or ""
    ).strip()

    # If only a full name is provided, split it
    if not first_name and not last_name:
        full_name = (
            lead.get("owner_name")
            or lead.get("owner")
            or lead.get("name")
            or ""
        ).strip()
        if full_name:
            parts = full_name.split(" ", 1)
            first_name = parts[0]
            last_name = parts[1] if len(parts) > 1 else ""

    if not first_name and not last_name:
        first_name = "Property Owner"

    # Extract phones
    phones = []
    for key in ["phone", "phone1", "phone_1", "owner_phone", "mobile", "cell"]:
        val = lead.get(key, "")
        if val and str(val).strip():
            phones.append({"type": "mobile", "value": str(val).strip()})
    for key in ["phone2", "phone_2", "phone3", "phone_3", "landline"]:
        val = lead.get(key, "")
        if val and str(val).strip():
            phones.append({"type": "home", "value": str(val).strip()})

    # Extract emails
    emails = []
    for key in ["email", "email1", "email_1", "owner_email"]:
        val = lead.get(key, "")
        if val and str(val).strip():
            emails.append({"type": "home", "value": str(val).strip()})
    for key in ["email2", "email_2", "email3"]:
        val = lead.get(key, "")
        if val and str(val).strip():
            emails.append({"type": "other", "value": str(val).strip()})

    # Build property address for notes
    address_parts = []
    for key in ["address", "property_address", "street", "street_address"]:
        val = lead.get(key, "")
        if val:
            address_parts.append(str(val).strip())
            break
    for key in ["city", "property_city"]:
        val = lead.get(key, "")
        if val:
            address_parts.append(str(val).strip())
            break
    for key in ["state", "property_state"]:
        val = lead.get(key, "")
        if val:
            address_parts.append(str(val).strip())
            break
    for key in ["zip", "zip_code", "postal_code", "property_zip"]:
        val = lead.get(key, "")
        if val:
            address_parts.append(str(val).strip())
            break

    property_address = ", ".join(address_parts) if address_parts else ""

    # Build tags from lead data
    tags = ["BatchLeads"]
    status = lead.get("status", "") or lead.get("lead_status", "")
    if status:
        tags.append(status)
    tag = lead.get("tag", "") or lead.get("tag_name", "")
    if tag:
        tags.append(tag)
    prop_type = lead.get("property_type", "")
    if prop_type:
        tags.append(prop_type)

    # Add equity/foreclosure tags if present
    if lead.get("in_foreclosure") or lead.get("is_foreclosure") or lead.get("pre_foreclosure"):
        tags.append("Pre-Foreclosure")
    equity = lead.get("equity_percent") or lead.get("equity_percentage")
    if equity:
        try:
            eq_val = float(str(equity).replace("%", ""))
            if eq_val >= 50:
                tags.append("High Equity")
        except (ValueError, TypeError):
            pass

    # Build background notes with property details
    notes_lines = []
    if property_address:
        notes_lines.append(f"Property: {property_address}")
    for label, keys in [
        ("Estimated Value", ["estimated_value", "market_value", "avm"]),
        ("Equity", ["equity_percent", "equity_percentage", "equity"]),
        ("Beds/Baths", ["bedrooms", "beds"]),
        ("Sq Ft", ["square_feet", "sqft", "living_area"]),
        ("Year Built", ["year_built"]),
        ("Last Sale", ["last_sale_date", "sale_date"]),
        ("Last Sale Price", ["last_sale_price", "sale_price"]),
        ("List", ["list_name", "list"]),
    ]:
        for key in keys:
            val = lead.get(key)
            if val:
                if label == "Beds/Baths":
                    baths = lead.get("bathrooms") or lead.get("baths") or ""
                    notes_lines.append(f"Beds/Baths: {val}/{baths}")
                else:
                    notes_lines.append(f"{label}: {val}")
                break

    background = "\n".join(notes_lines) if notes_lines else ""

    # Build FUB address
    fub_addresses = []
    if property_address:
        addr_obj = {"type": "other"}
        for key in ["address", "property_address", "street", "street_address"]:
            val = lead.get(key, "")
            if val:
                addr_obj["street"] = str(val).strip()
                break
        for key in ["city", "property_city"]:
            val = lead.get(key, "")
            if val:
                addr_obj["city"] = str(val).strip()
                break
        for key in ["state", "property_state"]:
            val = lead.get(key, "")
            if val:
                addr_obj["state"] = str(val).strip()
                break
        for key in ["zip", "zip_code", "postal_code", "property_zip"]:
            val = lead.get(key, "")
            if val:
                addr_obj["code"] = str(val).strip()
                break
        fub_addresses.append(addr_obj)

    # Create person in FUB with deduplication
    fub_payload = {
        "firstName": first_name,
        "lastName": last_name,
        "source": "BatchLeads",
        "tags": tags,
    }
    if phones:
        fub_payload["phones"] = phones
    if emails:
        fub_payload["emails"] = emails
    if fub_addresses:
        fub_payload["addresses"] = fub_addresses
    if background:
        fub_payload["background"] = background

    response = client.create_person(deduplicate=True, **fub_payload)

    person_id = response.get("id")
    return {
        "ok": True,
        "personId": person_id,
        "name": f"{first_name} {last_name}".strip(),
        "tags": tags,
        "address": property_address,
    }


# ---- LeadStream: Lead Priority Scoring API ----

@app.route("/api/leadstream/run", methods=["POST"])
def api_leadstream_run():
    """Run LeadStream scoring in background — returns job_id immediately."""
    import threading, uuid
    data = request.json or {}
    dry_run = data.get("dry_run", False)
    agent_name = data.get("agent", None)
    pond_only = data.get("pond_only", False)

    job_id = str(uuid.uuid4())[:8]
    _run_jobs[job_id] = {
        "status": "running",
        "started": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "pond_only": pond_only,
    }

    def _bg_run():
        try:
            from lead_scoring import LeadScorer, _get_leadstream_client
            client = _get_leadstream_client()
            scorer = LeadScorer(client)
            results = scorer.run(dry_run=dry_run, agent_name=agent_name, pond_only=pond_only)

            agent_count = len(results.get("agents", {}))
            total_agent_leads = sum(r["count"] for r in results["agents"].values())
            pond_count = len(results.get("pond", []))

            _run_jobs[job_id]["status"] = "complete"
            _run_jobs[job_id]["agents"] = {
                name: {
                    "count": info["count"],
                    "leads": [
                        {"name": l["name"], "score": l["score"], "tier": l["tier"]}
                        for l in info["leads"]
                    ],
                }
                for name, info in results.get("agents", {}).items()
            }
            _run_jobs[job_id]["pond"] = [
                {"name": l["name"], "score": l["score"], "tier": l["tier"]}
                for l in results.get("pond", [])
            ]
            _run_jobs[job_id]["tags_removed"] = results.get("removed", 0)
            _run_jobs[job_id]["api_requests"] = client.request_count
            _run_jobs[job_id]["summary"] = f"{total_agent_leads} agent leads + {pond_count} pond leads across {agent_count} agents"

            # ── Write engagement log entry immediately after each scoring run ──
            # This ensures the weekly tracker captures every scheduled run even
            # if nobody visits the dashboard that day. Actioned counts start at 0
            # and get updated (overwritten) next time the dashboard is viewed.
            try:
                import json as _ejson, tempfile as _etmp
                from datetime import datetime as _edt, timezone as _etz, timedelta as _etd
                _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
                _cache_base = (
                    os.environ.get("LEADSTREAM_CACHE_DIR")
                    or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
                )
                now_e = _edt.now(_etz.utc)
                run_key = now_e.isoformat()
                _run_agents = {
                    name: {"tagged": info["count"], "actioned": 0}
                    for name, info in results.get("agents", {}).items()
                }
                _run_pond = {"tagged": pond_count, "actioned": 0}

                if _db.is_available():
                    _db.write_engagement_entries(
                        run_key,
                        "pond" if pond_only else "full",
                        _run_agents,
                        _run_pond,
                    )
                else:
                    os.makedirs(_cache_base, exist_ok=True)
                    eng_log_path = os.path.join(_cache_base, "engagement_log.json")
                    eng_log = {}
                    for _rp in [eng_log_path, "/tmp/.cache/engagement_log.json"]:
                        try:
                            with open(_rp) as _f:
                                eng_log = _ejson.load(_f)
                            break
                        except Exception:
                            continue
                    if run_key not in eng_log:
                        eng_log[run_key] = {
                            "captured": now_e.isoformat(),
                            "mode": "pond" if pond_only else "full",
                            "agents": _run_agents,
                            "pond": _run_pond,
                            "total": total_agent_leads + pond_count,
                        }
                        cutoff = (now_e - _etd(days=30)).isoformat()
                        eng_log = {k: v for k, v in eng_log.items() if k >= cutoff}
                        _fd, _tmp2 = _etmp.mkstemp(dir=_cache_base, suffix=".tmp")
                        with os.fdopen(_fd, "w") as _f:
                            _ejson.dump(eng_log, _f)
                        os.replace(_tmp2, eng_log_path)
                        if _cache_base != "/tmp/.cache":
                            try:
                                os.makedirs("/tmp/.cache", exist_ok=True)
                                _fb, _ft = _etmp.mkstemp(dir="/tmp/.cache", suffix=".tmp")
                                with os.fdopen(_fb, "w") as _f:
                                    _ejson.dump(eng_log, _f)
                                os.replace(_ft, "/tmp/.cache/engagement_log.json")
                            except Exception:
                                pass
            except Exception as _ee:
                logger.warning("Could not write engagement log from scoring run: %s", _ee)

            # Bust dashboard cache
            _ls_dashboard_cache["data"] = None
            _ls_dashboard_cache["time"] = None

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.error("LeadStream run %s failed: %s\n%s", job_id, e, tb)
            _run_jobs[job_id]["status"] = "error"
            _run_jobs[job_id]["error"] = str(e)
            _run_jobs[job_id]["traceback"] = tb
            # Bust cache even on error so next dashboard load is fresh
            _ls_dashboard_cache["data"] = None
            _ls_dashboard_cache["time"] = None

    thread = threading.Thread(target=_bg_run, daemon=True)
    thread.start()

    return jsonify({"success": True, "job_id": job_id, "status": "running"})


@app.route("/api/leadstream/run/status/<job_id>")
def api_leadstream_run_status(job_id):
    """Poll status of a background LeadStream run."""
    job = _run_jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.route("/api/leadstream/deep-cleanup", methods=["POST"])
def api_leadstream_deep_cleanup():
    """Run a full deep cleanup of ALL LeadStream tags in FUB — for nightly use only.
    Runs in background thread. Returns job_id for polling via /run/status/<job_id>.
    """
    import threading, uuid
    job_id = str(uuid.uuid4())[:8]
    _run_jobs[job_id] = {
        "status": "running",
        "started": datetime.now(timezone.utc).isoformat(),
        "type": "deep_cleanup",
    }

    def _bg_cleanup():
        try:
            from lead_scoring import LeadScorer, _get_leadstream_client
            client = _get_leadstream_client()
            scorer = LeadScorer(client)
            removed = scorer.deep_cleanup(dry_run=False)
            _run_jobs[job_id]["status"] = "complete"
            _run_jobs[job_id]["removed"] = removed
            _run_jobs[job_id]["summary"] = f"Removed {removed} stale LeadStream tags"
            logger.info("Deep cleanup complete: %d tags removed", removed)
            # Bust dashboard cache
            _ls_dashboard_cache["data"] = None
            _ls_dashboard_cache["time"] = None
        except Exception as e:
            import traceback
            logger.error("Deep cleanup failed: %s", e)
            _run_jobs[job_id]["status"] = "error"
            _run_jobs[job_id]["error"] = str(e)
            _run_jobs[job_id]["traceback"] = traceback.format_exc()

    threading.Thread(target=_bg_cleanup, daemon=True).start()
    return jsonify({"success": True, "job_id": job_id, "status": "running"})


@app.route("/api/leadstream/debug")
def api_leadstream_debug():
    """Quick health check — tests imports, API key, and FUB connectivity."""
    import traceback as _tb
    result = {
        "env": {
            "FUB_API_KEY": "set" if os.environ.get("FUB_API_KEY") else "MISSING",
            "FUB_LEADSTREAM_API_KEY": "set" if os.environ.get("FUB_LEADSTREAM_API_KEY") else "not set (ok, will use FUB_API_KEY)",
            "RAILWAY_ENVIRONMENT": os.environ.get("RAILWAY_ENVIRONMENT", "not set"),
        },
        "import_lead_scoring": "pending",
        "fub_connection": "pending",
        "agent_count": None,
        "errors": [],
    }
    try:
        from lead_scoring import LeadScorer, _get_leadstream_client
        result["import_lead_scoring"] = "ok"
    except Exception as e:
        result["import_lead_scoring"] = f"FAILED: {e}"
        result["errors"].append(_tb.format_exc())
        return jsonify(result)
    try:
        client = _get_leadstream_client()
        users = client.get_users()
        result["fub_connection"] = "ok"
        result["agent_count"] = len(users)
    except Exception as e:
        result["fub_connection"] = f"FAILED: {e}"
        result["errors"].append(_tb.format_exc())
    return jsonify(result)


@app.route("/api/debug/calls")
def api_debug_calls():
    """Diagnose call fetch issues — shows raw FUB call data for the last 7 days."""
    try:
        from datetime import datetime, timezone, timedelta
        client = FUBClient()
        since = datetime.now(timezone.utc) - timedelta(days=7)
        until = datetime.now(timezone.utc)

        # Raw fetch — first page only, no date filtering, just to see what FUB returns
        raw = client._request("GET", "calls", params={"limit": 10, "sort": "-created"})
        sample_calls = raw.get("calls", [])

        # Full fetch with date range
        all_calls = client.get_calls(since=since, until=until)

        # Show field names present in first call
        first_call_fields = list(sample_calls[0].keys()) if sample_calls else []

        # Show userId distribution
        user_counts = {}
        for c in all_calls:
            uid = c.get("userId") or c.get("assignedUserId") or "none"
            user_counts[str(uid)] = user_counts.get(str(uid), 0) + 1

        # Show isIncoming distribution
        incoming_count = sum(1 for c in all_calls if c.get("isIncoming"))
        outbound_count = sum(1 for c in all_calls if not c.get("isIncoming"))

        # Agent map for comparison
        agent_map = auto_detect_agents(client)
        agent_ids = {name: user["id"] for name, user in agent_map.items()}

        return jsonify({
            "total_fetched_7d":    len(all_calls),
            "first_call_fields":   first_call_fields,
            "incoming_count":      incoming_count,
            "outbound_count":      outbound_count,
            "calls_by_userId":     user_counts,
            "agent_ids_in_system": agent_ids,
            "sample_call":         sample_calls[0] if sample_calls else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def api_health():
    """Health check endpoint — verifies scoring is running on schedule.

    Returns HTTP 200 with status 'ok' if last run was within expected window,
    HTTP 200 with status 'stale' if scoring is overdue (>5 hours since last run),
    HTTP 200 with status 'unknown' if manifest is missing (first run).
    """
    import json as _json
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    _is_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))
    _cache_base = (
        os.environ.get("LEADSTREAM_CACHE_DIR")
        or ("/tmp/.cache" if _is_railway else os.path.join(os.path.dirname(__file__), ".cache"))
    )
    MANIFEST_FILE = os.path.join(_cache_base, "leadstream_manifest.json")

    now = _dt.now(_tz.utc)
    manifest = None
    try:
        with open(MANIFEST_FILE) as f:
            manifest = _json.load(f)
    except Exception:
        pass
    if not manifest and _db.is_available():
        try:
            manifest = _db.read_manifest()
        except Exception:
            pass
    if manifest:
        last_run_str = manifest.get("last_run")
        last_run = _dt.fromisoformat(last_run_str) if last_run_str else None
        if last_run and last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=_tz.utc)
    else:
        last_run = None
        last_run_str = None

    if last_run is None:
        status = "unknown"
        age_hours = None
    else:
        age_hours = (now - last_run).total_seconds() / 3600
        # Scoring runs every 4h — allow up to 5h before flagging stale
        status = "ok" if age_hours <= 5 else "stale"

    # Scheduler status — use global _scheduler for accurate job details
    scheduler_info = {"running": _scheduler_started, "jobs": []}
    if _scheduler is not None:
        try:
            scheduler_info["jobs"] = [
                {
                    "id": j.id,
                    "name": j.name,
                    "next_run": str(j.next_run_time),
                    "last_fired": _job_last_fired.get(j.id, "never"),
                }
                for j in _scheduler.get_jobs()
            ]
        except Exception:
            pass

    # Cache status
    cache_status = {}
    for endpoint in ["audit", "manager", "isa", "appointments"]:
        cached = cache_get(endpoint)
        if cached:
            cache_status[endpoint] = {"cached": True, "cached_at": cached.get("cached_at")}
        else:
            cache_status[endpoint] = {"cached": False}

    # Env var checks — boolean only, never expose values
    env_checks = {
        "anthropic_api_key":    bool(os.environ.get("ANTHROPIC_API_KEY")),
        "sendgrid_api_key":     bool(os.environ.get("SENDGRID_API_KEY")),
        "heygen_api_key":       bool(os.environ.get("HEYGEN_API_KEY")),
        "fub_api_key":          bool(os.environ.get("FUB_API_KEY")),
        "database_url":         bool(os.environ.get("DATABASE_URL")),
        "project_blue_api_key": bool(os.environ.get("PROJECT_BLUE_API_KEY")),
    }

    return jsonify({
        "status": status,
        "last_leadstream_run": last_run_str,
        "leadstream_age_hours": round(age_hours, 1) if age_hours is not None else None,
        "agent_leads_tagged": len([
            i for leads in manifest.get("agent", {}).values()
            for i in leads
        ]) if last_run else 0,
        "pond_leads_tagged": len(manifest.get("pond", [])) if last_run else 0,
        "scheduler": scheduler_info,
        "dashboard_cache": cache_status,
        "env_checks": env_checks,
    })


@app.route("/api/leadstream/status")
def api_leadstream_status():
    """Check current LeadStream tagged leads."""
    try:
        from lead_scoring import LEADSTREAM_TAG, LEADSTREAM_POND_TAG
        client = FUBClient()
        agent_leads = client.get_people_by_tag(LEADSTREAM_TAG)
        pond_leads = client.get_people_by_tag(LEADSTREAM_POND_TAG)

        return jsonify({
            "agent_leads": [
                {
                    "id": p.get("id"),
                    "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                    "assignedTo": p.get("assignedUserId"),
                    "tags": p.get("tags", []),
                }
                for p in agent_leads
            ],
            "pond_leads": [
                {
                    "id": p.get("id"),
                    "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                    "tags": p.get("tags", []),
                }
                for p in pond_leads
            ],
            "agent_lead_count": len(agent_leads),
            "pond_lead_count": len(pond_leads),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---- Pond Mailer: LeadStream email marketing ----

@app.route("/api/pond-mailer/run", methods=["POST"])
def api_pond_mailer_run():
    """
    Trigger the pond mailer. Generates and sends personalized emails to
    LeadStream-tagged pond leads based on their IDX behavior.

    Body (all optional):
        dry_run      (bool, default true) — preview without sending
        person       (int)               — single FUB person ID for testing
        limit        (int)               — max leads to process this run
        daily_cap    (int)               — daily email ceiling (scheduler passes this)
        to_override  (str)               — redirect all sends to this address (test mode)
    """
    import threading, uuid
    data        = request.json or {}
    dry_run     = data.get("dry_run", True)
    person      = data.get("person")
    limit       = data.get("limit")
    daily_cap   = data.get("daily_cap")
    to_override = data.get("to_override")

    job_id = str(uuid.uuid4())[:8]

    # Persist job to DB so it survives Railway redeploys
    _db.ensure_pond_mailer_jobs_table()
    _db.create_pond_mailer_job(job_id, dry_run=dry_run)

    def _bg():
        try:
            from pond_mailer import run_pond_mailer
            result = run_pond_mailer(dry_run=dry_run, person_id=person, limit=limit, daily_cap=daily_cap, to_override=to_override)
            _db.finish_pond_mailer_job(job_id, result=result)
        except Exception as e:
            logger.error("Pond mailer error: %s", e)
            import traceback
            _db.finish_pond_mailer_job(job_id, error=f"{e}\n{traceback.format_exc()}")

    threading.Thread(target=_bg, daemon=True).start()

    return jsonify({"job_id": job_id, "status": "running", "dry_run": dry_run})


@app.route("/pond-admin")
def pond_admin_page():
    """Pond mailer admin dashboard — shows status, recent jobs, env checks, scheduler."""
    return render_template("pond_admin.html")


@app.route("/api/pond-mailer/status/<job_id>")
def api_pond_mailer_status(job_id):
    job = _db.get_pond_mailer_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.route("/api/pond-mailer/recent")
def api_pond_mailer_recent():
    """
    Activity feed for the pond mailer — what's been sent, what's in the queue.
    Reads from pond_email_log (reliable) and pond_mailer_jobs (best-effort).
    Useful for checking status without Railway log access.
    """
    try:
        import db as _db_local
        from datetime import datetime, timezone, timedelta
        from zoneinfo import ZoneInfo
        ET = ZoneInfo("America/New_York")
        now_et = datetime.now(ET)
        today_et = now_et.date()

        result = _db_local.get_pond_recent_activity()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pond-mailer/clear-today", methods=["POST"])
def api_pond_mailer_clear_today():
    """Admin: delete today's non-dry-run pond email log entries so the daily cap resets."""
    try:
        deleted = _db.delete_pond_emails_today()
        return jsonify({"deleted": deleted})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pond-mailer/new-lead-check", methods=["POST"])
def api_new_lead_check():
    """
    Check Shark Tank (pond 4) for new leads and fire the immediate
    'caught at the computer' email after a 10-12 minute delay.

    Body (optional):
        dry_run (bool, default true)
    """
    import threading
    data    = request.json or {}
    dry_run = data.get("dry_run", True)

    def _bg():
        try:
            from pond_mailer import run_new_lead_mailer
            result = run_new_lead_mailer(dry_run=dry_run)
            logger.info("New lead mailer: %s", result)
        except Exception as e:
            logger.error("New lead mailer error: %s", e)

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"status": "running", "dry_run": dry_run})


@app.route("/api/pond-mailer/stats")
def api_pond_mailer_stats():
    """Email send history and performance stats."""
    try:
        stats = _db.get_pond_email_stats(days=30)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/serendipity/run", methods=["POST"])
def api_serendipity_run():
    """
    Manually trigger the Serendipity Clause engine.

    Detects new behavioral triggers from FUB events and fires any pending
    triggers whose delay window has elapsed — in a background thread so the
    response returns immediately.

    Body (optional JSON):
        dry_run   bool   default true — set false for live sends
    """
    import threading
    data    = request.json or {}
    dry_run = data.get("dry_run", True)

    def _bg():
        try:
            from serendipity import run_serendipity
            result = run_serendipity(dry_run=dry_run)
            logger.info("Serendipity run complete: %s", result)
        except Exception as e:
            logger.error("Serendipity run error: %s", e, exc_info=True)

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"status": "running", "dry_run": dry_run})


@app.route("/api/serendipity/status")
def api_serendipity_status():
    """
    Serendipity Clause status: pending trigger count, last cursor timestamp,
    and 30-day send stats by trigger type.
    """
    try:
        stats  = _db.get_serendipity_stats(days=30)
        cursor = _db.get_serendipity_cursor()
        return jsonify({
            "cursor":      cursor.isoformat() if cursor else None,
            "stats_30d":   stats,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/heygen-bg")
def api_heygen_background():
    """
    Generate and serve a branded background image for HeyGen video rendering.

    HeyGen fetches this URL when rendering a personalized video. We generate
    the image on-demand from query params — no storage needed.

    Query params:
      type        — "seller", "zbuyer", or "buyer"
      address     — street address (seller: "1234 Oak Street")
      city        — city name ("Chesapeake")
      price_band  — buyer price band ("$350k–$500k")

    Returns: JPEG image
    """
    from flask import request, Response
    try:
        from heygen_client import (generate_seller_background_image,
                                   generate_buyer_background_image,
                                   generate_zbuyer_background_image)
        bg_type    = request.args.get("type", "seller")
        address    = request.args.get("address", "Your Home")
        city       = request.args.get("city", "Hampton Roads")
        price_band = request.args.get("price_band", "")

        cache_key = f"{bg_type}|{address}|{city}|{price_band}"
        cached    = _heygen_bg_cache.get(cache_key)
        if cached:
            return Response(cached, mimetype="image/jpeg",
                            headers={"Cache-Control": "public, max-age=3600"})

        if bg_type == "seller":
            img_bytes = generate_seller_background_image(address, city)
        elif bg_type == "zbuyer":
            img_bytes = generate_zbuyer_background_image(address, city)
        else:
            img_bytes = generate_buyer_background_image(city, price_band)

        _heygen_bg_cache[cache_key] = img_bytes
        return Response(img_bytes, mimetype="image/jpeg",
                        headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        logger.warning("heygen-bg generation failed: %s", e)
        from flask import abort
        abort(500)


# In-memory cache for generated background images — keyed by "type|address|city|price_band"
# Survives for the lifetime of the Railway process (hours to days).
# HeyGen fetches our bg URL at render time (~60s after submission); without caching,
# PIL regenerates on every request which takes 300-500ms.
_heygen_bg_cache: dict = {}

# ---------------------------------------------------------------------------
# Video thumbnail proxy — composites play button + duration badge onto the
# HeyGen thumbnail JPEG at open time, served from our Railway domain.
#
# This means heygen.com never appears in the email at all — not in href
# (handled by /v/<token>) and not in img src (handled here).
#
# The composited image shows Barry's face + a centered play button circle
# + a duration badge in the bottom-right corner, exactly like BombBomb/Vidyard.
# The play button is baked IN so it renders identically in every email client
# including Outlook (which strips CSS overlays).
# ---------------------------------------------------------------------------
_thumb_cache: dict = {}


@app.route("/thumb")
def video_thumb():
    """
    Proxy + composite a HeyGen thumbnail.

    Query params:
      t — base64url-encoded thumbnail URL (no padding)
      d — video duration in seconds (int, optional, default 0)

    Returns: JPEG with play button + duration badge composited in.
    """
    import base64, io, requests as _req
    from flask import Response as _R, abort as _abort
    from urllib.parse import unquote as _unquote

    raw_t = request.args.get("t", "").strip()
    dur   = max(0, int(request.args.get("d", 0) or 0))

    if not raw_t:
        _abort(400)

    cache_key = f"{raw_t}|{dur}"
    cached = _thumb_cache.get(cache_key)
    if cached:
        return _R(cached, mimetype="image/jpeg",
                  headers={"Cache-Control": "public, max-age=86400"})

    # Decode thumbnail URL
    try:
        padding = 4 - len(raw_t) % 4
        padded  = raw_t + ("=" * (padding if padding != 4 else 0))
        thumb_url = base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        _abort(400)

    if not thumb_url.startswith("https://"):
        _abort(400)

    # Fetch original thumbnail from HeyGen CDN
    try:
        resp = _req.get(thumb_url, timeout=8)
        resp.raise_for_status()
        img_bytes = resp.content
    except Exception as e:
        logger.warning("thumb proxy fetch failed: %s", e)
        _abort(502)

    # Composite play button + duration badge using PIL
    try:
        from PIL import Image, ImageDraw, ImageFont
        import math

        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        w, h = img.size

        draw = ImageDraw.Draw(img, "RGBA")

        # ── Play button circle ──────────────────────────────────────────────
        # Semi-transparent dark circle, centered
        cx, cy = w // 2, h // 2
        r = min(w, h) // 9          # radius scales with image size
        # Outer dark circle (semi-transparent)
        draw.ellipse(
            [cx - r, cy - r, cx + r, cy + r],
            fill=(0, 0, 0, 160),
        )
        # White play triangle — pointing right, centered in circle
        tri_size = int(r * 0.52)
        tx = cx - int(tri_size * 0.35)   # slight left-offset to visually center the triangle
        ty = cy
        triangle = [
            (tx,              ty - tri_size),
            (tx,              ty + tri_size),
            (tx + tri_size * 2, ty),
        ]
        draw.polygon(triangle, fill=(255, 255, 255, 240))

        # ── Duration badge ──────────────────────────────────────────────────
        if dur > 0:
            mins  = dur // 60
            secs  = dur % 60
            dur_str = f"{mins}:{secs:02d}"

            # Try to load a font; fall back to PIL default
            font = None
            for fp in [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "/System/Library/Fonts/Helvetica.ttc",
                "/Library/Fonts/Arial.ttf",
            ]:
                try:
                    font = ImageFont.truetype(fp, size=max(14, h // 22))
                    break
                except Exception:
                    continue
            if font is None:
                font = ImageFont.load_default()

            bbox    = draw.textbbox((0, 0), dur_str, font=font)
            tw, th  = bbox[2] - bbox[0], bbox[3] - bbox[1]
            pad     = max(5, h // 55)
            margin  = max(8, h // 45)
            bx      = w - tw - pad * 2 - margin
            by      = h - th - pad * 2 - margin
            # Dark pill background
            draw.rounded_rectangle(
                [bx, by, bx + tw + pad * 2, by + th + pad * 2],
                radius=4, fill=(0, 0, 0, 200),
            )
            draw.text((bx + pad, by + pad), dur_str,
                      fill=(255, 255, 255, 255), font=font)

        # Re-encode as JPEG
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
        result = buf.getvalue()

    except Exception as e:
        logger.warning("thumb composite failed: %s — serving original", e)
        result = img_bytes   # fall back to un-composited thumbnail

    _thumb_cache[cache_key] = result
    return _R(result, mimetype="image/jpeg",
              headers={"Cache-Control": "public, max-age=86400"})


@app.route("/watch")
def watch_video():
    """
    HTML5 video player landing page for HeyGen email thumbnails.

    Desktop email clients download a raw .mp4 link instead of playing it.
    Mobile native players handle it fine, but desktop does not.

    Fix: thumbnail hrefs in emails point here (/watch?url=<encoded_mp4>).
    This page renders a full-screen HTML5 <video> player that autoplays
    the clip inline — works across desktop Chrome, Safari, Firefox, Outlook.

    Query param:
      url — the HeyGen MP4 video URL (https:// only, URL-encoded)
    """
    import html as _html
    from urllib.parse import unquote as _unquote
    video_url = request.args.get("url", "").strip()
    if not video_url:
        return "Missing video URL.", 400
    # Decode if the caller double-encoded (some email clients do this)
    if video_url.startswith("http%"):
        video_url = _unquote(video_url)
    if not video_url.startswith("https://"):
        return "Invalid video URL.", 400
    safe_url = _html.escape(video_url)
    html_out = (
        "<!DOCTYPE html>"
        "<html lang='en'><head>"
        "<meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1.0'>"
        "<title>Barry Jenkins \u2014 Personal Message</title>"
        "<style>"
        "*{margin:0;padding:0;box-sizing:border-box}"
        "body{background:#0c1228;display:flex;flex-direction:column;"
        "align-items:center;justify-content:center;min-height:100vh;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif}"
        ".wrap{width:100%;max-width:900px;padding:16px}"
        "video{width:100%;border-radius:10px;box-shadow:0 8px 40px rgba(0,0,0,0.6);cursor:pointer}"
        ".brand{margin-top:14px;text-align:center;"
        "color:rgba(255,255,255,0.35);font-size:13px;letter-spacing:0.03em}"
        "</style></head><body>"
        "<div class='wrap'>"
        f"<video id='v' src='{safe_url}' controls autoplay playsinline preload='auto'>"
        f"<a href='{safe_url}' style='color:#fff'>Download the video</a>"
        "</video>"
        "<p class='brand'>Legacy Home Team &nbsp;&middot;&nbsp; Barry Jenkins, Realtor"
        " &nbsp;&middot;&nbsp; LPT Realty</p>"
        "</div>"
        "<script>"
        "var v=document.getElementById('v');"
        "v.play().catch(function(){v.controls=true;});"
        "v.addEventListener('click',function(){v.paused?v.play():v.pause();});"
        "</script>"
        "</body></html>"
    )
    from flask import Response as _R
    return _R(html_out, status=200, mimetype="text/html")


# Cache resolved HeyGen CDN URLs keyed by video_id — avoids a HeyGen API
# call on every iOS range request. TTL=90 min (HeyGen signed URLs expire in ~2h).
_vp_url_cache: dict = {}   # video_id → (cdn_url, expires_at)

def _get_cdn_url(video_id: str) -> str | None:
    """Return cached CDN URL for video_id, refreshing if expired."""
    import time, requests as _req
    cached = _vp_url_cache.get(video_id)
    if cached and cached[1] > time.time():
        return cached[0]
    try:
        _hg_key = os.environ.get("HEYGEN_API_KEY", "")
        r = _req.get(
            "https://api.heygen.com/v1/video_status.get",
            headers={"X-Api-Key": _hg_key},
            params={"video_id": video_id},
            timeout=8,
        )
        _data = r.json().get("data", {})
        if _data.get("status") != "completed":
            return None
        cdn_url = _data.get("video_url", "")
        if cdn_url:
            _vp_url_cache[video_id] = (cdn_url, time.time() + 5400)  # 90-min TTL
        return cdn_url or None
    except Exception as _e:
        logger.warning("_get_cdn_url failed for %s: %s", video_id, _e)
        return None


@app.route("/audio/<audio_id>")
def serve_audio(audio_id: str):
    """
    Serve a generated ElevenLabs voice note for Project Blue delivery.

    Audio is stored in-memory in elevenlabs_client._audio_store with a 10-minute
    TTL. Project Blue fetches this URL as audioAttachmentUrl when sending the
    iMessage audio bubble. The file only needs to be accessible for a few seconds.
    """
    try:
        import elevenlabs_client as _el
        audio_bytes = _el.get_audio(audio_id)
        if not audio_bytes:
            return "", 404
        from flask import Response as _Resp
        return _Resp(
            audio_bytes,
            status=200,
            mimetype="audio/mpeg",
            headers={
                "Content-Disposition": f'attachment; filename="voicenote-{audio_id}.mp3"',
                "Content-Length": str(len(audio_bytes)),
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        logger.error("Audio serve failed for %s: %s", audio_id, e)
        return "", 500


@app.route("/mthumb/<video_id>")
def mms_thumb(video_id: str):
    """
    MMS-optimised thumbnail for Twilio MMS delivery.

    HeyGen thumbnails are 1920×1080 JPEG at ~900KB — too large for carrier
    MMS limits (~600KB). This endpoint fetches the thumbnail via HeyGen API,
    resizes to 640×360, and re-compresses to quality 55 (~120–180KB).

    Cached in memory for 2 hours (same session as /vp/ CDN cache).
    Used as media_url in pond_mailer MMS sends instead of the raw video.
    """
    import io, re as _re
    from flask import Response as _R, abort as _abort
    import requests as _req

    # Normalise video_id
    vid = video_id.lower().strip()
    if not _re.match(r'^[0-9a-f]{32}$', vid):
        _abort(400)

    cache_key = f"mthumb:{vid}"
    cached = _thumb_cache.get(cache_key)
    if cached:
        return _R(cached, mimetype="image/jpeg",
                  headers={"Cache-Control": "public, max-age=7200"})

    # Fetch HeyGen thumbnail URL
    hg_key = os.environ.get("HEYGEN_API_KEY", "")
    if not hg_key:
        _abort(503)

    try:
        r = _req.get(
            "https://api.heygen.com/v1/video_status.get",
            headers={"X-Api-Key": hg_key, "Content-Type": "application/json"},
            params={"video_id": vid},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json().get("data", {})
        if data.get("status") != "completed":
            _abort(404)
        thumb_url = data.get("thumbnail_url", "")
        if not thumb_url:
            _abort(404)
    except Exception as _e:
        logger.warning("mms_thumb HeyGen lookup failed for %s: %s", vid, _e)
        _abort(502)

    # Fetch original thumbnail
    try:
        resp = _req.get(thumb_url, timeout=10)
        resp.raise_for_status()
        raw_bytes = resp.content
    except Exception as _e:
        logger.warning("mms_thumb fetch failed for %s: %s", vid, _e)
        _abort(502)

    # Resize + compress with PIL → target ≤ 400 KB for carrier MMS delivery
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
        # Resize to 640×360 (16:9, standard MMS resolution)
        img = img.resize((640, 360), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=55, optimize=True)
        jpeg_bytes = buf.getvalue()
        logger.info("mms_thumb %s: %d KB (compressed from %d KB)",
                    vid, len(jpeg_bytes) // 1024, len(raw_bytes) // 1024)
    except Exception as _e:
        logger.warning("mms_thumb PIL failed for %s: %s — serving raw", vid, _e)
        jpeg_bytes = raw_bytes  # serve original if PIL fails

    _thumb_cache[cache_key] = jpeg_bytes
    return _R(jpeg_bytes, mimetype="image/jpeg",
              headers={"Cache-Control": "public, max-age=7200"})


@app.route("/vp/<video_id>")
def video_proxy(video_id: str):
    """
    iOS-compatible video proxy — streams the HeyGen MP4 through our server
    with proper HTTP Range request support.

    iOS Safari probes with Range: bytes=0-1 before playing inline. HeyGen's
    CloudFront URLs return 200 (not 206) for range requests, so Safari stalls.
    This proxy intercepts range requests and returns proper 206 responses.

    CDN URL is cached per video_id (90-min TTL) so iOS's multiple range
    requests don't each re-hit the HeyGen API — cuts load time from ~15s → ~2s.

    Timeout: (5s connect, no read timeout) so long videos aren't cut off.
    """
    import re
    import requests as _req
    from flask import request, Response as _R, stream_with_context

    if not re.match(r'^[0-9a-f]{32}$', video_id.lower()):
        return "Not found.", 404

    cdn_url = _get_cdn_url(video_id.lower())
    if not cdn_url or not cdn_url.startswith("https://"):
        return "Not found.", 404

    # Forward Range header — iOS sends bytes=0-1 probe, then real range requests
    upstream_headers = {"Accept": "*/*"}
    range_hdr = request.headers.get("Range")
    if range_hdr:
        upstream_headers["Range"] = range_hdr

    try:
        upstream = _req.get(
            cdn_url,
            headers=upstream_headers,
            stream=True,
            timeout=(5, None),   # 5s connect; no read timeout (don't cut off video)
        )
    except Exception as _e:
        logger.warning("video_proxy upstream fetch failed: %s", _e)
        return "Upstream error.", 502

    resp_headers = {
        "Content-Type":  upstream.headers.get("Content-Type", "video/mp4"),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
    }
    for hdr in ("Content-Length", "Content-Range", "Last-Modified", "ETag"):
        if hdr in upstream.headers:
            resp_headers[hdr] = upstream.headers[hdr]

    return _R(
        stream_with_context(upstream.iter_content(chunk_size=131072)),  # 128KB chunks
        status=upstream.status_code,
        headers=resp_headers,
    )


@app.route("/go/<code>")
def short_video_url(code: str):
    """
    Short URL redirect for SMS video links.

    /go/<first-8-chars-of-video_id>  →  /v/<full-video_id>?c=lat,lon,zoom

    Keeps SMS links short (~50 chars vs 95+ for full URL).
    Stored in SQLite (short_video_urls table) — survives Railway redeploys.
    """
    from flask import redirect as _redirect
    import re as _re
    c = code.lower().strip()
    if not _re.match(r'^[0-9a-f]{6,32}$', c):
        return "Not found.", 404
    target = _db_get_short_url(c)
    if not target:
        # Fallback: if code is a full 32-char video_id, go directly
        if len(c) == 32:
            return _redirect(f"/v/{c}", code=302)
        return "Not found.", 404
    return _redirect(target, code=302)


def _ensure_short_url_table():
    """Create short_video_urls SQLite table if it doesn't exist."""
    try:
        conn = _db._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS short_video_urls (
                code       TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    except Exception as _e:
        logger.warning("_ensure_short_url_table: %s", _e)


def _db_get_short_url(code: str) -> str | None:
    try:
        conn = _db._get_conn()
        row = conn.execute(
            "SELECT target FROM short_video_urls WHERE code = ?", (code,)
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _db_set_short_url(code: str, target: str):
    try:
        _ensure_short_url_table()
        conn = _db._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO short_video_urls (code, target) VALUES (?, ?)",
            (code, target),
        )
        conn.commit()
    except Exception as _e:
        logger.warning("_db_set_short_url failed: %s", _e)


def make_short_video_url(video_id: str, map_center: str = "") -> str:
    """
    Register a short URL in SQLite and return the full /go/<code> URL.

    code = first 8 chars of video_id — unique enough for our send volume.
    map_center = "lat,lon,zoom" appended as ?c= param on the landing page.
    Persists across Railway redeploys via SQLite.
    """
    base = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")
    code   = video_id.lower()[:8]
    target = f"/v/{video_id.lower()}"
    if map_center:
        target += f"?c={map_center}"
    _db_set_short_url(code, target)
    return f"{base}/go/{code}"


@app.route("/v/<token>")
def video_landing(token):
    """
    Clean video landing page — hides heygen.com from email bodies.

    Two URL formats are supported:
      /v/<video_id>  — preferred: token is a 32-char HeyGen video UUID.
                       Route calls HeyGen API to get a fresh signed URL.
                       Video is served via /vp/<video_id> proxy for iOS compat.
                       Produced by make_video_landing_url(url, video_id=id).
                       Example URL: /v/726f98a0956d434889663419f22c4060  (79 chars)

      /v/<b64token>  — fallback: token is a base64url-encoded raw MP4 URL.
                       Used for older emails generated without video_id.
                       Example URL: /v/aHR0cHM6Ly9maW...  (250+ chars)

    Performance notes:
      - video_id path routes video through /vp/ proxy — iOS Safari range-request compat
      - preload='auto' so iOS buffers aggressively (metadata alone stalls on Safari)
      - poster=thumbnail so the frame shows immediately while video initialises
    """
    import re, base64, html as _html

    video_url     = None
    thumbnail_url = None
    use_proxy     = False   # True when we have a video_id → can use /vp/ proxy

    # ── Path 1: HeyGen video_id (32 lowercase hex chars) ──────────────────
    if re.match(r'^[0-9a-f]{32}$', token.lower()):
        try:
            import requests as _req
            _hg_key = os.environ.get("HEYGEN_API_KEY", "")
            if _hg_key:
                r = _req.get(
                    "https://api.heygen.com/v1/video_status.get",
                    headers={"X-Api-Key": _hg_key, "Content-Type": "application/json"},
                    params={"video_id": token.lower()},
                    timeout=8,
                )
                if r.status_code == 200:
                    _data = r.json().get("data", {})
                    if _data.get("status") == "completed":
                        video_url     = _data.get("video_url", "")
                        thumbnail_url = _data.get("thumbnail_url", "")
                        use_proxy     = True   # serve via /vp/ for iOS compat
        except Exception as _e:
            logger.warning("video_landing HeyGen lookup failed: %s", _e)

    # ── Path 2: base64url-encoded raw URL (legacy / fallback) ─────────────
    if not video_url:
        try:
            padding      = 4 - len(token) % 4
            token_padded = token + ("=" * (padding if padding != 4 else 0))
            video_url    = base64.urlsafe_b64decode(token_padded).decode("utf-8", errors="strict")
        except Exception:
            return "Not found.", 404

    if not video_url or not video_url.startswith("https://"):
        return "Not found.", 404

    # Route through proxy for iOS when we have a video_id
    base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if base_url:
        base_url = f"https://{base_url}"
    else:
        from flask import request as _freq
        base_url = _freq.host_url.rstrip("/")

    if use_proxy:
        playback_url = f"{base_url}/vp/{token.lower()}"
    else:
        playback_url = video_url   # legacy b64 path — no proxy available

    safe_url    = _html.escape(playback_url)
    poster_attr = f"poster=\"{_html.escape(thumbnail_url)}\"" if thumbnail_url else ""

    # ── Map center from ?c=lat,lon,zoom query param (set by make_video_landing_url) ──
    from flask import request as _freq
    _map_c = _freq.args.get("c", "36.8531,-76.2859,11")
    try:
        _parts  = _map_c.split(",")
        map_lat  = float(_parts[0])
        map_lon  = float(_parts[1])
        map_zoom = float(_parts[2]) if len(_parts) > 2 else 11.0
    except Exception:
        map_lat, map_lon, map_zoom = 36.8531, -76.2859, 11.0

    # Mapbox public access token — safe to embed in HTML (restrict by referrer in dashboard)
    mapbox_token = os.environ.get("MAPBOX_ACCESS_TOKEN", "")

    # ── Build landing page: map fades behind dark overlay, video is the centered hero ──
    # Map provides locality context; dark overlay stops it competing with the video.
    # Falls back gracefully: if no Mapbox token, renders the classic dark-background player.
    if mapbox_token:
        html_out = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0">
<title>Barry Jenkins — Personal Message</title>
<script src="https://api.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.js"></script>
<link href="https://api.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.css" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{overflow:hidden;background:#0c1228;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif}}

/* Map — full screen background, non-interactive so video gets all taps */
#map{{position:fixed;top:0;left:0;width:100vw;height:100vh;z-index:0;pointer-events:none}}

/* Dark overlay — fades the map so the video is the clear focal point */
#overlay{{
  position:fixed;top:0;left:0;width:100vw;height:100vh;
  background:rgba(10,16,36,0.72);
  z-index:1;pointer-events:none;
}}

/* Video — centered hero, large */
#stage{{
  position:fixed;top:0;left:0;width:100vw;height:100vh;
  display:flex;flex-direction:column;
  align-items:center;justify-content:center;
  z-index:10;padding:16px;
}}
#vid-wrap{{
  width:100%;max-width:560px;
  border-radius:14px;overflow:hidden;
  box-shadow:0 12px 48px rgba(0,0,0,0.7);
  background:#000;
}}
#vid-wrap video{{display:block;width:100%;max-height:80vh;}}

/* Brand — below video */
#brand{{
  margin-top:12px;
  color:rgba(255,255,255,0.45);font-size:12px;letter-spacing:0.03em;
  text-align:center;text-shadow:0 1px 4px rgba(0,0,0,0.8);
}}

/* Mobile: full-width */
@media(max-width:480px){{
  #vid-wrap{{border-radius:10px}}
  #brand{{font-size:11px}}
}}
</style>
</head>
<body>

<div id="map"></div>
<div id="overlay"></div>

<div id="stage">
  <div id="vid-wrap">
    <video id="v" src="{safe_url}" {poster_attr} controls playsinline preload="auto">
      <a href="{safe_url}" style="color:#fff;padding:16px;display:block">Watch video</a>
    </video>
  </div>
  <div id="brand">Legacy Home Team &nbsp;&middot;&nbsp; Barry Jenkins, Realtor &nbsp;&middot;&nbsp; LPT Realty</div>
</div>

<script>
// Map loads in background — non-interactive, purely atmospheric
mapboxgl.accessToken = '{_html.escape(mapbox_token)}';
new mapboxgl.Map({{
  container: 'map',
  style: 'mapbox://styles/mapbox/navigation-day-v1',
  center: [{map_lon:.6f}, {map_lat:.6f}],
  zoom: {map_zoom:.1f},
  interactive: false,
  attributionControl: false,
}});

// Video autoplay on desktop; iOS requires user tap
var v = document.getElementById('v');
if (!('ontouchstart' in window)) {{
  v.play().catch(function(){{}});
}}
</script>
</body>
</html>"""
    else:
        # Fallback: classic dark-background player (no Mapbox token)
        html_out = (
            "<!DOCTYPE html>"
            "<html lang='en'><head>"
            "<meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1.0'>"
            "<title>Barry Jenkins — Personal Message</title>"
            "<style>"
            "*{margin:0;padding:0;box-sizing:border-box}"
            "body{background:#0c1228;display:flex;flex-direction:column;"
            "align-items:center;justify-content:center;min-height:100vh;"
            "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif}"
            ".wrap{width:100%;max-width:900px;padding:16px}"
            "video{width:100%;border-radius:10px;box-shadow:0 8px 40px rgba(0,0,0,0.6);cursor:pointer}"
            ".brand{margin-top:14px;text-align:center;"
            "color:rgba(255,255,255,0.35);font-size:13px;letter-spacing:0.03em}"
            "</style></head><body>"
            "<div class='wrap'>"
            f"<video id='v' src='{safe_url}' {poster_attr} controls playsinline preload='auto'>"
            f"<a href='{safe_url}' style='color:#fff'>Download the video</a>"
            "</video>"
            "<p class='brand'>Legacy Home Team &nbsp;&middot;&nbsp; Barry Jenkins, Realtor"
            " &nbsp;&middot;&nbsp; LPT Realty</p>"
            "</div>"
            "<script>"
            "var v=document.getElementById('v');"
            "if(!('ontouchstart' in window)){"
            "v.addEventListener('click',function(){v.paused?v.play():v.pause();});"
            "v.play().catch(function(){});"
            "}"
            "</script>"
            "</body></html>"
        )

    from flask import Response as _R
    return _R(html_out, status=200, mimetype="text/html")


@app.route("/api/ask-claude", methods=["POST"])
def api_ask_claude():
    """
    Command Center AI Q&A — Barry asks a question about the data he sees.

    Accepts:
        { "question": str, "context": str (optional — data snippet from the UI) }

    Returns:
        { "answer": str }

    The endpoint injects a system prompt that frames Claude as Barry's
    coaching partner, gives it team context (role, style, Virginia market),
    and passes the question + optional data context. Responses are conversational,
    actionable, and aligned with Barry's "too nice" coaching philosophy.
    """
    import anthropic as _anthropic
    body = request.get_json(force=True, silent=True) or {}
    question = (body.get("question") or "").strip()
    extra_ctx = (body.get("context") or "").strip()

    if not question:
        return jsonify({"error": "question is required"}), 400

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 500

    system_prompt = (
        "You are Barry Jenkins' real estate coaching partner. "
        "Barry leads Legacy Home Team in Virginia Beach/Chesapeake/Suffolk — "
        "Virginia's #1 real estate team, closing 850+ homes per year. "
        "He runs a small team of agents and tracks their KPIs (calls, conversations, "
        "appointment set rate, show rate, contract rate) weekly. "
        "He uses a coaching philosophy from his book 'Too Nice for Sales': "
        "never shame agents, always reframe, teaching over pushing, story-first, "
        "conversational tone, actionable endings. "
        "His ISA (Inside Sales Agent) is Joe — handles call volume accountability. "
        "Conversion gaps (opener, ask, close) are Barry's domain. "
        "When answering, be direct and specific. No fluff. "
        "Give 2-4 bullet points of actionable coaching advice max. "
        "Keep responses under 250 words. Plain text, no markdown headers. "
        "If the question references specific data, use it directly in your answer."
    )

    user_content = question
    if extra_ctx:
        user_content = f"Current dashboard data:\n{extra_ctx}\n\nQuestion: {question}"

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=512,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        answer = msg.content[0].text if msg.content else "No response generated."
        return jsonify({"answer": answer})
    except Exception as e:
        logger.error("ask-claude error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/privacy-policy")
def privacy_policy():
    return """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Privacy Policy — Legacy Home Team</title>
<style>
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:700px;margin:40px auto;padding:0 24px;color:#1a1a1a;line-height:1.7}
  h1{font-size:1.6rem;margin-bottom:4px}
  h2{font-size:1.1rem;margin-top:32px;margin-bottom:8px}
  p,li{font-size:.95rem}
  .updated{color:#666;font-size:.85rem;margin-bottom:32px}
</style>
</head>
<body>
<h1>Privacy Policy</h1>
<p class="updated">Legacy Home Team (Friend in Realty LLC) &mdash; Last updated April 2026</p>

<h2>1. Who We Are</h2>
<p>Legacy Home Team, operating as Friend in Realty LLC, is a licensed real estate team based in Hampton Roads, Virginia. This policy covers our internal team communication program used to send coaching, performance, and goal-tracking messages to our licensed agents via SMS.</p>

<h2>2. Information We Collect</h2>
<p>We collect and use the following information solely for internal team communications:</p>
<ul>
  <li>Agent name and cell phone number (provided directly by the agent upon joining the team)</li>
  <li>Performance data including call counts, appointment totals, and GCI figures</li>
</ul>

<h2>3. How We Use Your Information</h2>
<p>Phone numbers and performance data are used exclusively to send internal coaching messages, daily goal updates, weekly KPI summaries, and motivational content from team leader Barry Jenkins. This information is never sold, rented, or shared with third parties for marketing purposes.</p>

<h2>4. SMS Communications</h2>
<p>All SMS recipients are licensed agents who are active team members and who have explicitly consented to receive team communications. Recipients may opt out at any time by replying <strong>STOP</strong> to any message. For help, reply <strong>HELP</strong>.</p>

<h2>5. Data Retention</h2>
<p>Agent contact information is retained for the duration of active employment with the team and removed upon departure.</p>

<h2>6. Contact</h2>
<p>Questions about this policy: barry@yourfriendlyagent.net &mdash; Legacy Home Team, Hampton Roads, Virginia.</p>
</body>
</html>""", 200, {"Content-Type": "text/html"}


@app.route("/terms")
def terms_and_conditions():
    return """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Terms &amp; Conditions — Legacy Home Team SMS</title>
<style>
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:700px;margin:40px auto;padding:0 24px;color:#1a1a1a;line-height:1.7}
  h1{font-size:1.6rem;margin-bottom:4px}
  h2{font-size:1.1rem;margin-top:32px;margin-bottom:8px}
  p,li{font-size:.95rem}
  .updated{color:#666;font-size:.85rem;margin-bottom:32px}
  strong{font-weight:700}
</style>
</head>
<body>
<h1>SMS Terms &amp; Conditions</h1>
<p class="updated">Legacy Home Team (Friend in Realty LLC) &mdash; Last updated April 2026</p>

<h2>Program Description</h2>
<p>Legacy Home Team operates an internal SMS coaching program that sends performance updates, goal tracking, KPI summaries, and motivational coaching messages to licensed real estate agents employed by Friend in Realty LLC in Hampton Roads, Virginia.</p>

<h2>Who Receives Messages</h2>
<p>Only licensed agents who are active team members and who have provided their personal cell phone number directly to team leader Barry Jenkins are included. Participation requires explicit consent at the time of joining the team.</p>

<h2>Message Frequency</h2>
<p>Agents may receive up to 2&ndash;3 SMS messages per day. Frequency varies based on performance milestones, daily coaching cycles, and weekly summaries.</p>

<h2>Message &amp; Data Rates</h2>
<p>Message and data rates may apply. Contact your wireless carrier for details about your plan.</p>

<h2>How to Opt Out</h2>
<p>Reply <strong>STOP</strong> to any message at any time to unsubscribe. You will receive a confirmation message and no further SMS will be sent.</p>

<h2>How to Get Help</h2>
<p>Reply <strong>HELP</strong> to any message or contact us at barry@yourfriendlyagent.net.</p>

<h2>Support Contact</h2>
<p>Barry Jenkins &mdash; Legacy Home Team<br>barry@yourfriendlyagent.net<br>Hampton Roads, Virginia</p>

<h2>Privacy</h2>
<p>Your information is never sold or shared with third parties. See our full <a href="/privacy-policy">Privacy Policy</a>.</p>
</body>
</html>""", 200, {"Content-Type": "text/html"}


@app.route("/unsubscribe", methods=["GET"])
def pond_unsubscribe():
    """
    One-click unsubscribe for pond nurture emails.

    URL format: /unsubscribe?e=BASE64URL_EMAIL
    - Decodes the email address from the token
    - Looks up the lead in pond_email_log
    - Tags them PondMailer_Unsubscribed in FUB (suppresses future sends)
    - Shows a plain confirmation page

    RFC 8058 / Gmail one-click compliant — no email compose required.
    """
    import base64 as _b64
    from flask import Response as _R

    raw_token = request.args.get("e", "").strip()
    if not raw_token:
        return _R("<h2>Invalid unsubscribe link.</h2>", status=400, mimetype="text/html")

    # Decode email — base64url, padding-tolerant
    try:
        padded = raw_token + "=" * (-len(raw_token) % 4)
        email  = _b64.urlsafe_b64decode(padded).decode("utf-8").strip().lower()
        if "@" not in email:
            raise ValueError("not an email")
    except Exception:
        return _R("<h2>Invalid unsubscribe link.</h2>", status=400, mimetype="text/html")

    # Look up person_id from pond_email_log
    person_id, person_name, _ = _db.get_pond_email_person_by_email(email)
    already_done = False

    if person_id:
        try:
            from fub_client import FUBClient
            fub = FUBClient()
            fub.add_tag(person_id, "PondMailer_Unsubscribed")
            logger.info("One-click unsubscribe: tagged %s (ID %s) PondMailer_Unsubscribed", email, person_id)
        except Exception as exc:
            logger.warning("One-click unsubscribe: FUB tag failed for %s: %s", email, exc)
    else:
        # Email not in pond log — may have already been unsubscribed or never emailed
        already_done = True
        logger.info("One-click unsubscribe: no pond record for %s", email)

    # Log the unsubscribe in pond_reply_log so it shows in the dashboard
    if person_id:
        try:
            _db.log_pond_reply(
                person_id=person_id,
                person_name=person_name or email,
                reply_from=email,
                reply_text="[One-click unsubscribe via email footer link]",
                sentiment="negative",
                sentiment_score=1.0,
                routed=False,
                fub_task_id=None,
            )
        except Exception:
            pass

    # Confirmation page
    display = person_name.split()[0] if person_name else "there"
    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Unsubscribed — Legacy Home Team</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
      background: #f8f8f8;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      margin: 0;
    }}
    .card {{
      background: #fff;
      border-radius: 12px;
      padding: 40px 48px;
      max-width: 480px;
      text-align: center;
      box-shadow: 0 2px 16px rgba(0,0,0,0.08);
    }}
    h1 {{ font-size: 22px; color: #1a1a1a; margin-bottom: 12px; }}
    p  {{ font-size: 15px; color: #555; line-height: 1.6; margin-bottom: 8px; }}
    .check {{ font-size: 48px; margin-bottom: 16px; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="check">✅</div>
    <h1>You've been unsubscribed</h1>
    <p>Hey {display} — you're all set. We won't send you any more emails from this list.</p>
    <p style="color:#aaa;font-size:13px;margin-top:24px;">Legacy Home Team &nbsp;·&nbsp; Barry Jenkins</p>
  </div>
</body>
</html>"""
    return _R(page, status=200, mimetype="text/html")


@app.route("/api/pond-mailer/reply", methods=["POST"])
def api_pond_mailer_reply():
    """
    SendGrid Inbound Parse webhook — receives email replies from pond leads.

    SendGrid POSTs multipart/form-data to this endpoint when a lead hits
    Reply on one of Barry's outreach emails. We:
      1. Extract the from address and body
      2. Look up the lead in pond_email_log by email address
      3. Analyze sentiment with Claude (haiku — cheap and fast)
      4. If positive: create a FUB task for the assigned agent + add a note
      5. If negative (opt-out): tag PondMailer_Unsubscribed to suppress future sends
      6. Log everything to pond_reply_log

    Always returns 200 — SendGrid will retry on non-2xx and we don't want
    that for unrecoverable failures (unmatched address, API errors, etc.).
    """
    try:
        # SendGrid Inbound Parse sends multipart/form-data
        from_raw  = request.form.get("from", "")
        subject   = request.form.get("subject", "")
        body_text = request.form.get("text", "") or ""
        # Fall back to HTML if no plain text (strip tags crudely)
        if not body_text.strip():
            import re as _re2
            html_body = request.form.get("html", "")
            body_text = _re2.sub(r"<[^>]+>", " ", html_body).strip()

        # Parse "Name <email>" → "email"
        import re
        m = re.search(r"<([^>]+)>", from_raw)
        clean_from = m.group(1).strip() if m else from_raw.strip()

        logger.info("Pond reply webhook: from=%s | subject=%s", clean_from, subject[:80])

        if not clean_from or "@" not in clean_from:
            logger.warning("Pond reply: unparseable from address: %s", from_raw)
            return jsonify({"status": "ok", "action": "bad_from"}), 200

        # Match reply to a pond lead by their email address
        person_id, person_name, reply_seq_num = _db.get_pond_email_person_by_email(clean_from)

        if not person_id:
            logger.info("Pond reply: no matching pond lead for %s", clean_from)
            return jsonify({"status": "ok", "action": "unmatched"}), 200

        logger.info("Pond reply matched: %s (ID: %s) — replied to Email %s",
                    person_name, person_id, reply_seq_num)

        # Analyze sentiment
        sentiment, sentiment_score, sentiment_reason = _pond_analyze_sentiment(
            body_text, person_name
        )
        logger.info("Sentiment: %s (%.2f) — %s", sentiment, sentiment_score, sentiment_reason)

        routed       = False
        task_id      = None
        assigned_uid = None
        fub_note_ok  = False

        if sentiment == "positive":
            try:
                from fub_client import FUBClient
                fub = FUBClient()

                # Tag the lead — FUB automation handles assignment (only fires for pond leads,
                # skips if an agent already claimed them to prevent double-assignment).
                fub.add_tag(person_id, "Email_Conversion")
                logger.info("Email_Conversion tag applied to lead %s (replied to Email %s)",
                            person_id, reply_seq_num)
                routed = True

                # Add FUB note so the timeline shows what triggered the conversion,
                # including which sequence email generated the reply.
                fub_note_ok = _pond_add_fub_note(
                    fub, person_id, person_name, body_text, subject,
                    sentiment_reason, seq_num=reply_seq_num
                )

            except Exception as e:
                logger.error("FUB positive reply handling failed for %s (ID %s): %s", person_name, person_id, e)

        elif sentiment == "negative":
            # Tag lead to suppress from future pond sends
            try:
                from fub_client import FUBClient
                fub = FUBClient()
                fub.add_tag(person_id, "PondMailer_Unsubscribed")
                logger.info("Tagged %s as PondMailer_Unsubscribed", person_name)
            except Exception as e:
                logger.warning("Could not tag unsubscribe for %s: %s", person_name, e)

        else:
            # Neutral — still log a FUB note so the reply is visible in CRM
            try:
                from fub_client import FUBClient
                fub = FUBClient()
                fub_note_ok = _pond_add_fub_note(fub, person_id, person_name, body_text, subject, sentiment_reason)
            except Exception as e:
                logger.warning("Could not add FUB note for neutral reply (person %s): %s", person_id, e)

        # Log the reply
        _db.log_pond_reply(
            person_id=person_id,
            person_name=person_name,
            reply_from=clean_from,
            reply_text=body_text[:2000],
            sentiment=sentiment,
            sentiment_score=sentiment_score,
            routed=routed,
            fub_task_id=task_id,
        )

        # ── Notify Barry of the reply ──────────────────────────────────────
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail as _SGMail
            sg_key = os.environ.get("SENDGRID_API_KEY")
            if sg_key:
                _emoji = {"positive": "🔥", "negative": "🚫", "neutral": "💬"}.get(sentiment, "📬")
                _subj  = f"{_emoji} Reply from {person_name or clean_from} — {sentiment.upper()}"

                _lines = [
                    f"{person_name or 'A lead'} ({clean_from}) replied to your AI outreach email.",
                    "",
                    f"Subject they replied to: {subject}",
                    f"Sentiment: {sentiment.upper()} — {sentiment_reason}",
                    "",
                    "─── Their reply ───────────────────────────────────",
                    body_text.strip()[:1200],
                    "────────────────────────────────────────────────────",
                    "",
                ]
                if routed:
                    _seq_str = f" (triggered by Email {reply_seq_num})" if reply_seq_num else ""
                    _lines.append(f"✅ Email_Conversion tag applied{_seq_str} — FUB automation will assign")
                    _lines.append(f"✅ CRM note added: {'yes' if fub_note_ok else 'check logs'}")
                elif sentiment == "negative":
                    _lines.append("🚫 Tagged PondMailer_Unsubscribed — lead will be suppressed from future sends")
                else:
                    _lines.append(f"📝 CRM note added: {'yes' if fub_note_ok else 'check logs'}")

                _lines += ["", "— Legacy Home Team AI Outreach"]
                _notify_body = "\n".join(_lines)
                _notify_html = (
                    "<div style='font-family:-apple-system,sans-serif;font-size:15px;"
                    "line-height:1.7;color:#222;max-width:560px;margin:24px auto'>"
                    + _notify_body.replace("\n", "<br>") +
                    "</div>"
                )
                _sg_msg = _SGMail(
                    from_email=config.EMAIL_FROM,
                    to_emails=config.EMAIL_FROM,
                    subject=_subj,
                    plain_text_content=_notify_body,
                    html_content=_notify_html,
                )
                SendGridAPIClient(sg_key).send(_sg_msg)
                logger.info("Barry notified of %s reply from %s", sentiment, clean_from)
        except Exception as _ne:
            logger.warning("Could not notify Barry of reply: %s", _ne)
        # ──────────────────────────────────────────────────────────────────

        return jsonify({
            "status":      "ok",
            "person_id":   person_id,
            "person_name": person_name,
            "sentiment":   sentiment,
            "routed":      routed,
            "fub_note_ok": fub_note_ok,
        }), 200

    except Exception as e:
        logger.error("Pond reply webhook unhandled error: %s", e, exc_info=True)
        # Always 200 — we don't want SendGrid to retry unrecoverable errors
        return jsonify({"status": "ok", "error": str(e)}), 200


def _is_consent_reply(body_text: str) -> bool:
    """
    Return True if the lead's reply is a consent to receive the recording
    Barry offered ("would it be ok if i sent a quick recording?").

    Intentionally loose — catches any clear "yes" signal without requiring
    the exact word "yes". Does NOT use AI; pure pattern match so it never
    costs tokens and never times out.

    Distinct from buying sentiment: "ok" alone is consent but not necessarily
    buying intent. Both can be true at once.
    """
    if not body_text:
        return False

    text = body_text.lower().strip()
    # Remove trailing punctuation for exact-match tests
    text_clean = text.rstrip("!. ")

    # Exact single-token consents
    _CONSENT_EXACT = {
        "yes", "yeah", "yea", "yep", "yup", "ya", "yah",
        "ok", "okay", "k", "kk",
        "sure", "sure thing",
        "go ahead", "go for it", "go",
        "please", "please do",
        "sounds good", "sounds great", "sounds perfect",
        "absolutely", "definitely", "of course",
        "for sure", "forsure",
        "alright", "alrighty",
        "send it", "send away", "shoot",
        "do it", "let's go", "lets go",
        "i'm in", "im in",
        "that works", "works for me",
        "yes please", "yes absolutely", "yes please do",
        "of course please", "by all means",
        "👍", "✅", "💯", "🙌",
    }

    if text_clean in _CONSENT_EXACT:
        return True

    # Starts-with patterns (catches "yes that would be great", "sure go ahead", etc.)
    _CONSENT_STARTS = (
        "yes ", "yeah ", "yep ", "yup ", "sure ", "ok ", "okay ",
        "absolutely ", "definitely ", "of course ", "go ahead",
        "please send", "please do", "please go",
        "sounds good", "sounds great",
        "for sure", "yes please",
    )
    if any(text.startswith(p) for p in _CONSENT_STARTS):
        return True

    return False


def _pond_analyze_sentiment(reply_text, person_name=""):
    """
    Use Claude Haiku to classify a reply as positive / neutral / negative.
    Falls back to keyword detection if the API key isn't set.
    Returns (sentiment, score, reason).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if not api_key:
        # Keyword fallback
        lower = reply_text.lower()
        OPT_OUT = ["unsubscribe", "stop", "remove me", "don't contact",
                   "do not contact", "not interested", "leave me alone",
                   "opt out", "opt-out", "take me off"]
        POSITIVE = ["yes", "interested", "show me", "tell me", "love to",
                    "would love", "can we", "let's", "when can", "available",
                    "sounds good", "that's great", "want to", "looking to buy",
                    "ready to", "schedule", "appointment", "call me"]
        if any(w in lower for w in OPT_OUT):
            return "negative", 0.9, "Opt-out keywords detected"
        if any(w in lower for w in POSITIVE):
            return "positive", 0.7, "Interest keywords detected"
        return "neutral", 0.5, "No clear signal (keyword fallback — no ANTHROPIC_API_KEY)"

    try:
        import anthropic, json as _json, re
        client = anthropic.Anthropic(api_key=api_key)

        prompt = f"""Analyze this email reply from a real estate prospect named {person_name or "a lead"}.

They received an outreach email from Barry Jenkins (Legacy Home Team, Virginia Beach VA)
referencing homes they browsed on our site. Classify their reply sentiment:

REPLY:
{reply_text[:1200]}

Classify as:
- positive: Shows genuine interest, asks questions, wants to see homes or talk, or any meaningful engagement
- neutral: Polite but non-committal, unclear intent, vague acknowledgment
- negative: Unsubscribe/opt-out, "not interested", "stop emailing me", angry or firm rejection

Return JSON only (no markdown):
{{"sentiment":"positive"|"neutral"|"negative","score":0.0-1.0,"reason":"one sentence"}}"""

        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)
        data = _json.loads(raw)

        return (
            data.get("sentiment", "neutral"),
            float(data.get("score", 0.5)),
            data.get("reason", ""),
        )
    except Exception as e:
        logger.warning("Sentiment analysis failed: %s", e)
        return "neutral", 0.5, f"Analysis error: {e}"


def _pond_add_fub_note(fub, person_id, person_name, reply_text, original_subject,
                       sentiment_reason, seq_num=None):
    """
    Generate an engaging FUB note using Claude and post it to the lead.
    The note clearly flags this as an AI email reply, which email triggered
    the reply (seq_num), summarizes what the lead said, and gives the agent
    a concrete recommended action.
    Falls back to a plain template if Claude is unavailable.
    Returns True if note was posted successfully, False otherwise.
    """
    seq_label = f"Email {seq_num} of their automated sequence" if seq_num else "an automated email"

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        note_body = None

        if api_key:
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)

                prompt = f"""Write an internal CRM note for a real estate agent who just got a hot lead hand-off.

Context:
- Lead name: {person_name or "this lead"}
- Barry Jenkins (Legacy Home Team, Hampton Roads VA) sent them {seq_label} via his AI nurture system
- Subject line of the email: "{original_subject}"
- The lead replied positively. AI sentiment: {sentiment_reason}
- Their reply: {reply_text.strip()[:800]}

Structure the note in three short sections with these exact labels:
WHAT HAPPENED: (1 sentence — which email in the sequence triggered this, what the AI read from the reply)
WHAT THEY SAID: (2-3 sentence summary of the lead's actual reply in plain language)
YOUR MOVE: (One specific, actionable opening line for the agent's call, then one question to advance toward consult, showing, or next step)

Rules:
- Write as Barry briefing the agent, direct and energetic
- Under 200 words total
- No fluff, no greetings, no sign-offs
- Plain text only, no markdown"""

                response = client.messages.create(
                    model="claude-3-5-haiku-20241022",
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}],
                )
                note_body = response.content[0].text.strip()
            except Exception as e:
                logger.warning("Claude note generation failed: %s", e)

        # Fallback if Claude unavailable or fails
        if not note_body:
            note_body = (
                f"🔥 HOT LEAD — {person_name or 'This lead'} replied to Barry's AI nurture system.\n\n"
                f"WHAT HAPPENED:\n"
                f"Barry's system sent {seq_label} with subject \"{original_subject}\".\n"
                f"The lead replied. AI sentiment: {sentiment_reason}\n\n"
                f"WHAT THEY SAID:\n{reply_text.strip()[:600]}\n\n"
                f"YOUR MOVE:\n"
                f"Call them today — they're warm. Open with: "
                f"\"Hey, I saw you replied to Barry's email — I'm his agent and I wanted to "
                f"reach out personally.\" Then ask what caught their attention and get them "
                f"booked for a consult or showing."
            )

        fub._request("POST", "notes", json_data={
            "personId": int(person_id),
            "body":     note_body,
        })
        logger.info("FUB reply note added for person %s (Email %s)", person_id, seq_num)
        return True
    except Exception as e:
        logger.warning("Could not add FUB note for reply (person %s): %s", person_id, e)
        return False


def _pond_add_sms_reply_fub_note(fub, person_id, person_name, reply_text, sentiment_reason):
    """
    Generate a rich CRM briefing note for an inbound SMS reply and post it to FUB.

    Pulls three layers of context so the agent walks into the call prepared:
      1. The original AI text we sent (what hook we used)
      2. The lead's current behavioral brief (what they've viewed, saved, price range, etc.)
      3. A specific 'YOUR MOVE' opener + one question to advance toward a showing or consult

    Returns True if note was posted successfully, False otherwise.
    """
    try:
        # ── 1. Pull the original SMS we sent ─────────────────────────────────
        original_sms = None
        try:
            original_sms = _db.get_last_sms_sent(person_id)
        except Exception as _e:
            logger.warning("Could not retrieve original SMS for note: %s", _e)

        # ── 2. Re-fetch behavioral data from FUB ─────────────────────────────
        behavior_lines = []
        try:
            from pond_mailer import analyze_behavior
            events = fub.get_events_for_person(person_id, days=90, limit=100)
            tags   = []  # tags not critical for behavior analysis here
            beh    = analyze_behavior(events, tags)
            b      = beh

            # Most-viewed property — the #1 hook to reference on the call
            if b.get("most_viewed") and b.get("most_viewed_ct", 0) >= 2:
                p    = b["most_viewed"]
                addr = f"{p.get('street')}, {p.get('city')}" if p.get("city") else p.get("street", "")
                pval = p.get("price")
                price_str = f" (${_safe_int(pval):,})" if pval else ""
                behavior_lines.append(
                    f"  * VIEWED {b['most_viewed_ct']}x: {addr}{price_str} "
                    f"-- they keep coming back to this one. Open your call with this address."
                )
            elif b.get("registration_prop"):
                rp    = b["registration_prop"]
                raddr = f"{rp.get('street')}, {rp.get('city')}" if rp.get("city") else rp.get("street", "")
                rpval = rp.get("price")
                rprice = f" (${_safe_int(rpval):,})" if rpval else ""
                behavior_lines.append(
                    f"  * REGISTERED ON: {raddr}{rprice} "
                    f"-- this is the property that made them give us their contact info."
                )

            # Saved properties — second strongest signal
            if b.get("saves"):
                for sp in b["saves"][:3]:
                    sa    = f"{sp.get('street')}, {sp.get('city')}" if sp.get("city") else sp.get("street", "")
                    spval = sp.get("price")
                    sprice = f" (${_safe_int(spval):,})" if spval else ""
                    behavior_lines.append(f"  * SAVED: {sa}{sprice}")

            # Price range + drift
            if b.get("price_min") and b.get("price_max"):
                price_line = f"  * PRICE RANGE: ${b['price_min']:,} to ${b['price_max']:,}"
                if b.get("price_drift") and abs(b["price_drift"]) > 15000:
                    direction = "UP" if b["price_drift"] > 0 else "DOWN"
                    price_line += (
                        f" -- search has drifted {direction} ${abs(b['price_drift']):,} "
                        f"from where they started"
                    )
                behavior_lines.append(price_line)

            # Cities
            cities_list = sorted(b.get("cities") or [])
            if cities_list:
                if len(cities_list) == 1:
                    behavior_lines.append(f"  * CITY: locked into {cities_list[0]}")
                else:
                    behavior_lines.append(f"  * SEARCHING IN: {', '.join(cities_list[:4])}")

            # Beds
            beds = sorted(b.get("beds_seen") or [])
            if beds:
                if len(beds) > 1:
                    behavior_lines.append(f"  * BEDS: looking at {min(beds)}-{max(beds)} bedrooms")
                else:
                    behavior_lines.append(f"  * BEDS: {beds[0]} bedroom focus")

            # Engagement depth
            vc   = b.get("view_count", 0)
            sc   = b.get("session_count", 0)
            savc = b.get("save_count", 0)
            if vc:
                behavior_lines.append(
                    f"  * SEARCH DEPTH: {vc} total views across {sc} separate sessions, {savc} saves"
                )

            # Recency
            if b.get("hours_since_last") is not None:
                hrs = b["hours_since_last"]
                if hrs < 24:
                    behavior_lines.append(f"  * LAST ACTIVE: {int(hrs)}h ago -- still hot")
                elif hrs < 72:
                    behavior_lines.append(f"  * LAST ACTIVE: {int(hrs/24)} days ago")
                else:
                    behavior_lines.append(f"  * LAST ACTIVE: {int(hrs/24)} days ago")

        except Exception as _be:
            logger.warning("Behavior re-fetch for FUB note failed (non-fatal): %s", _be)

        # ── 3. Build the agent delivery guide via Claude Haiku ───────────────
        # This is the core: decode what the AI text promised, then give the agent
        # the ACTUAL substance to deliver on it using only what we genuinely know.
        delivery_guide = None
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if api_key:
                import anthropic as _ant2
                _ant_cl = _ant2.Anthropic(api_key=api_key)

                _beh_summary = "\n".join(behavior_lines) if behavior_lines else "No behavioral data available."
                _orig_hook   = f'"{original_sms}"' if original_sms else "not available"

                _delivery_prompt = f"""You are briefing a real estate agent who just got a reply to an AI nurture text.
The agent needs to know: (1) exactly what the AI text implied, (2) what they can ACTUALLY say to
deliver on that implication using ONLY the real behavioral data we have, and (3) a specific opener.

IMPORTANT: We only have behavioral data from the lead's activity on our IDX website (view counts,
saves, price range, search patterns). We do NOT have MLS data, seller history, or property records.
The agent must deliver on the promise using behavioral insights only -- which are genuine and powerful.

Lead name: {person_name or "this lead"}
Our AI text: {_orig_hook}
Lead replied: "{reply_text.strip()[:400]}"
What our AI detected: {sentiment_reason}

Their actual behavioral data (this is all we know -- use it):
{_beh_summary}

Write three sections:

WHAT OUR TEXT IMPLIED:
One sentence: what expectation did our text create? What did the lead think we know?

WHAT YOU ACTUALLY KNOW (say this when they ask "what did you find?"):
2-3 specific things the agent can say, built ONLY from the behavioral data above.
These must be true, specific, and interesting. Frame behavioral patterns as insights.
Example frames that work:
  - "When someone views the same house X times, it usually means [specific interpretation]"
  - "Your search moved up $X from where you started -- that tells me [specific thing]"
  - "You saved [address] but kept looking -- usually means [specific interpretation]"

YOUR MOVE:
OPENER: One sentence for the agent to open with -- references something specific, not generic.
QUESTION: One question that moves toward a consult, showing, or commitment.

Under 200 words total. Direct and useful. No fluff."""

                _delivery_resp = _ant_cl.messages.create(
                    model="claude-3-5-haiku-20241022",
                    max_tokens=350,
                    messages=[{"role": "user", "content": _delivery_prompt}],
                )
                delivery_guide = _delivery_resp.content[0].text.strip()
        except Exception as _oe:
            logger.warning("Claude delivery guide generation failed (non-fatal): %s", _oe)

        # ── 4. Assemble the full note ─────────────────────────────────────────
        note_parts = [
            f"REPLY TO AI TEXT -- {sentiment_reason.upper()}",
            "",
            "WHAT THEY SAID:",
            f'"{reply_text.strip()[:600]}"',
            "",
        ]

        if original_sms:
            note_parts += [
                "HOOK WE USED:",
                f'"{original_sms[:400]}"',
                "",
            ]

        if behavior_lines:
            note_parts += [
                "THEIR BEHAVIORAL DATA (everything we actually know):",
            ] + behavior_lines + [
                "",
                "TO SEE THEIR FULL HISTORY: Open their FUB profile > Activity tab > IDX Activity.",
                "Every property they viewed, saved, and searched is there.",
                "",
            ]

        if delivery_guide:
            note_parts += [
                "---",
                delivery_guide,
            ]
        else:
            # Fallback when Claude is unavailable
            _first = (person_name or "there").split()[0]
            _addr_hint = ""
            for bl in behavior_lines[:3]:
                if any(x in bl for x in ("VIEWED", "SAVED", "REGISTERED")):
                    _addr_hint = bl.split(":", 1)[-1].strip().split("--")[0].strip()
                    break
            note_parts += [
                "---",
                "WHAT YOU ACTUALLY KNOW (say this when they ask 'what did you find?'):",
                "  The behavioral data above is your substance. Frame it as insight:",
                "  'When someone keeps going back to the same house that many times, it usually",
                "   means there's one specific thing they can't answer from the photos. What is it?'",
                "  'Your search has moved up in price from where you started -- that tells me",
                "   something about what you've been finding vs. what you actually want.'",
                "",
                "YOUR MOVE:",
                f"OPENER: 'Hey {_first}, it's [your name] from Barry's team."
                + (f" I saw you replied about {_addr_hint}." if _addr_hint
                   else " I saw you replied to our text and wanted to reach out directly.'"),
                "QUESTION: What's the one thing about this search that hasn't clicked yet?",
            ]

        note_body = "\n".join(note_parts)

        fub._request("POST", "notes", json_data={
            "personId": int(person_id),
            "body":     note_body,
        })
        logger.info("FUB SMS reply note added for person %s", person_id)
        return True
    except Exception as e:
        logger.warning("Could not add FUB note for SMS reply (person %s): %s", person_id, e)
        return False


def _generate_handoff_sms(lead_first, reply_text, agent_first, agent_phone):
    """
    Use Claude Haiku to write a personalized, warm handoff SMS.

    This is the final impression Barry's AI makes before the human agent takes
    over — it needs to feel genuine, create confidence, and make the lead
    genuinely excited to hear from the agent. Not a boilerplate transfer message.

    Falls back to a strong hand-crafted template if Claude is unavailable.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    # ── Strong fallback templates (used when Claude unavailable) ────────────
    # Written so they don't sound like automation. Multiple variants so repeat
    # leads don't see the same text.
    import random
    if agent_first and agent_phone:
        fallbacks = [
            (f"{lead_first}, really glad you reached out. I'm handing you to {agent_first} — "
             f"they're one of the best on our team and already have your full context. "
             f"Expect a call from {agent_phone} very soon."),
            (f"{lead_first}, love the response. Connecting you with {agent_first} now — "
             f"they specialize in exactly this and will reach out from {agent_phone} "
             f"shortly. You're in good hands."),
            (f"{lead_first}, this is exactly why I do this. Sending {agent_first} your "
             f"way — they'll call from {agent_phone} and already know your situation. "
             f"This conversation is going to be worth it."),
        ]
    elif agent_first:
        fallbacks = [
            (f"{lead_first}, really glad you replied. Handing you to {agent_first} — "
             f"they're one of the best we have and already know your situation. "
             f"Expect their direct call shortly."),
            (f"{lead_first}, connecting you with {agent_first} now. They have full context "
             f"on what you're looking for and will reach out from their direct line. "
             f"This is the conversation worth having."),
        ]
    else:
        fallbacks = [
            (f"{lead_first}, glad you replied. One of our top agents is being connected "
             f"to your file right now — they'll reach out from their direct line shortly "
             f"with full context on your situation."),
        ]

    if not api_key:
        return random.choice(fallbacks)

    # ── Claude Haiku — personalized, reads the actual reply ─────────────────
    phone_line = f"Agent's direct number: {agent_phone}" if agent_phone else "Agent's direct number: not yet assigned"
    try:
        import anthropic as _ant
        client = _ant.Anthropic(api_key=api_key)

        prompt = f"""You are Barry Jenkins' AI assistant at Legacy Home Team — Virginia's #1 real estate team (850+ homes/year, Hampton Roads VA).

A lead just replied positively to one of Barry's AI outreach texts. You're sending a final handoff SMS before a human agent takes over the conversation.

CONTEXT:
Lead's first name: {lead_first}
What the lead just said: "{reply_text[:300]}"
Assigned agent's first name: {agent_first or "our agent"}
{phone_line}

WRITE a 35-45 word handoff SMS that:
1. Opens with the lead's first name + a single warm sentence that acknowledges their reply authentically (NOT generic — it should feel like you actually read what they said)
2. Introduces the agent by first name as the specific expert being sent their way — make them sound worth talking to
3. If you have the agent's phone: mention it naturally as "they'll reach out from [number]"
4. Closes with one line that creates genuine anticipation — something that makes them want to pick up when the agent calls

RULES:
— 35-45 words. No more.
— NO "just", "reaching out", "checking in", "feel free", "happy to help", "don't hesitate"
— NO corporate-speak. This should read like Barry typed it himself.
— Start with the lead's first name followed by a comma
— No sign-off (automatically added). No links.
— The agent should sound like the best person in the world for their specific situation, not a random handoff

Output ONLY the SMS text. Nothing else."""

        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=130,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # Strip any accidental sign-off
        for stop in ("Barry Jenkins", "— Barry", "Legacy Home Team"):
            idx = text.find(stop)
            if idx > 0:
                text = text[:idx].strip()
        if len(text) > 20:
            return text
        # Claude returned something too short — use fallback
        logger.warning("Handoff SMS generation returned suspiciously short text: %r", text)
        return random.choice(fallbacks)

    except Exception as _ce:
        logger.warning("Claude handoff SMS generation failed: %s — using fallback", _ce)
        return random.choice(fallbacks)


def _schedule_sms_handoff(person_id, to_phone, reply_text="", lead_first_name="",
                          delay_seconds=270):
    """
    Fire a personalized handoff SMS to the lead after a positive/consent reply.

    Default delay is 270s (4.5 min) for buying-intent replies, giving FUB
    automation time to apply SMS_Conversion and assign the lead to an agent.
    For consent-only replies (no buying signal yet) caller passes 900s so the
    lead has time to listen to the voice note before we check in.

      1. Fetches the assigned agent's name + direct phone from FUB
      2. Uses Claude Haiku to write a warm, specific handoff message
      3. Sends via Project Blue (iMessage-first)

    Runs in a daemon thread — Flask response returns immediately.
    """
    import threading

    def _send():
        try:
            from fub_client import FUBClient
            import projectblue_client as _pb_handoff
            fub = FUBClient()

            # Fetch updated lead to see who FUB automation assigned
            person = fub.get_person(person_id)
            uid    = (person.get("assignedUserId") or
                      person.get("ownerId") or
                      (person.get("assignedTo") or {}).get("id"))

            agent_first = None
            agent_phone = None

            if uid:
                agent = fub.get_user_by_id(uid)
                if agent:
                    agent_first = (agent.get("firstName") or
                                   (agent.get("name") or "").split()[0] or None)
                    agent_phone = (agent.get("mobilePhone") or
                                   agent.get("phone") or
                                   agent.get("phoneNumber") or None)

            lead_first = lead_first_name or "there"

            # Generate the handoff message via Claude (with strong fallback)
            body = _generate_handoff_sms(
                lead_first=lead_first,
                reply_text=reply_text,
                agent_first=agent_first,
                agent_phone=agent_phone,
            )

            result = _pb_handoff.send_message(to_phone, body, dry_run=False)
            if result.get("success"):
                logger.info("Handoff iMessage sent to %s (person %s, agent: %s, %d chars)",
                            to_phone, person_id, agent_first or "unassigned", len(body))
            else:
                logger.warning("Handoff iMessage failed for person %s: %s",
                               person_id, result.get("error"))
        except Exception as _e:
            logger.error("SMS handoff timer failed for person %s: %s",
                         person_id, _e, exc_info=True)

    t = threading.Timer(delay_seconds, _send)
    t.daemon = True
    t.start()
    logger.info("Handoff SMS scheduled in %ds for person %s → %s",
                delay_seconds, person_id, to_phone)


# CTIA-standard opt-out keywords — Twilio handles these at the carrier level,
# but we hard-gate on them before running Claude sentiment so a STOP reply
# always applies SMS_OptOut immediately, even if the Anthropic API is down.
_SMS_HARD_STOP_WORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}


@app.route("/webhook/sendblue", methods=["POST"])
def webhook_sendblue():
    """
    Sendblue inbound iMessage webhook — receives blue-bubble replies from pond leads.

    Sendblue POSTs JSON to this endpoint when a lead replies to an outbound
    iMessage or when delivery status updates occur.

    Flow mirrors the Twilio SMS webhook:
      1. Parse JSON body — filter to inbound received messages only
      2. Hard-gate STOP keywords (opt-out before Claude runs)
      3. Match phone number to lead via pond_sms_log
      4. Claude Haiku sentiment analysis
      5. Positive  → SMS_Conversion + FUB note + handoff SMS (4.5 min)
      6. Negative  → SMS_OptOut tag
      7. Neutral   → FUB note only
      8. Log to pond_sms_reply_log + notify Barry

    Always returns 200 so Sendblue doesn't retry.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}

    # Sendblue sends status callbacks (QUEUED, DELIVERED, etc.) and inbound messages.
    # We only care about inbound received messages.
    status = (data.get("status") or "").upper()
    if status not in ("RECEIVED", ""):
        return jsonify({"ok": True, "skipped": "status_callback"}), 200

    from_number = data.get("from_number") or data.get("number") or ""
    body_text   = (data.get("content") or "").strip()
    media_url   = data.get("media_url") or ""

    if not from_number or not body_text:
        return jsonify({"ok": True, "skipped": "no_content"}), 200

    logger.info("Sendblue inbound from %s: %r", from_number, body_text[:80])

    # ── Hard-gate STOP keywords ───────────────────────────────────────────────
    word = body_text.strip().lower().split()[0] if body_text.strip() else ""
    if word in _SMS_HARD_STOP_WORDS:
        try:
            from fub_client import FUBClient
            from pond_mailer import _db as _pdb
            fub = FUBClient()
            pid, pname = _pdb.find_person_by_phone(from_number)
            if pid:
                fub.add_tag_fast(pid, "SMS_OptOut", [])
                _pdb.log_pond_sms_reply(
                    person_id=pid, person_name=pname or "", phone=from_number,
                    reply_text=body_text, sentiment="negative",
                    sentiment_score=1.0, sentiment_reason="STOP keyword",
                    channel="sendblue",
                )
                logger.info("Sendblue STOP from %s (person %s) — SMS_OptOut applied", from_number, pid)
        except Exception as _e:
            logger.error("Sendblue STOP handling failed: %s", _e)
        return jsonify({"ok": True, "opted_out": True}), 200

    # ── Match phone to lead ───────────────────────────────────────────────────
    try:
        from pond_mailer import _db as _pdb
        person_id, person_name = _pdb.find_person_by_phone(from_number)
    except Exception as _e:
        logger.error("Sendblue: phone lookup failed for %s: %s", from_number, _e)
        person_id, person_name = None, None

    if not person_id:
        logger.warning("Sendblue: no lead matched for phone %s", from_number)
        return jsonify({"ok": True, "skipped": "no_lead_match"}), 200

    # ── Sentiment analysis ────────────────────────────────────────────────────
    try:
        sentiment, sentiment_score, sentiment_reason = _pond_analyze_sentiment(
            body_text, person_name=person_name or ""
        )
    except Exception as _e:
        logger.error("Sendblue sentiment failed for %s: %s", person_id, _e)
        sentiment, sentiment_score, sentiment_reason = "neutral", 0.5, "analysis error"

    logger.info("Sendblue sentiment: %s (%.2f) — %s", sentiment, sentiment_score, sentiment_reason)

    # ── Route by sentiment ────────────────────────────────────────────────────
    try:
        from fub_client import FUBClient
        fub = FUBClient()

        if sentiment == "positive":
            fub.add_tag_fast(person_id, "SMS_Conversion", [])
            _pond_add_fub_note(fub, person_id, person_name, body_text,
                               "iMessage Reply", sentiment_reason)
            _schedule_sms_handoff(
                person_id=person_id, to_phone=from_number,
                reply_text=body_text, lead_first_name=(person_name or "").split()[0],
            )

        elif sentiment == "negative":
            fub.add_tag_fast(person_id, "SMS_OptOut", [])
            _pond_add_fub_note(fub, person_id, person_name, body_text,
                               "iMessage Reply", sentiment_reason)

        else:  # neutral
            _pond_add_fub_note(fub, person_id, person_name, body_text,
                               "iMessage Reply", sentiment_reason)

    except Exception as _e:
        logger.error("Sendblue FUB routing failed for %s: %s", person_id, _e)

    # ── Log reply ─────────────────────────────────────────────────────────────
    try:
        from pond_mailer import _db as _pdb
        _pdb.log_pond_sms_reply(
            person_id=person_id, person_name=person_name or "",
            phone=from_number, reply_text=body_text,
            sentiment=sentiment, sentiment_score=sentiment_score,
            sentiment_reason=sentiment_reason, channel="sendblue",
        )
    except Exception as _e:
        logger.warning("Sendblue reply log failed: %s", _e)

    # ── Notify Barry ──────────────────────────────────────────────────────────
    try:
        _emoji = {"positive": "🔥", "negative": "🚫", "neutral": "💬"}.get(sentiment, "📱")
        _subj  = f"{_emoji} iMessage reply from {person_name or from_number} — {sentiment.upper()}"
        _body  = "\n".join([
            f"Lead: {person_name} ({from_number})",
            f"Sentiment: {sentiment.upper()} — {sentiment_reason}",
            f"Message: {body_text}",
        ])
        _notify_barry_of_reply(subject=_subj, body=_body, sentiment=sentiment)
    except Exception as _e:
        logger.warning("Sendblue Barry notification failed: %s", _e)

    return jsonify({
        "ok":         True,
        "person_id":  person_id,
        "sentiment":  sentiment,
        "channel":    "sendblue",
    }), 200


@app.route("/webhook/twilio-sms", methods=["POST"])
def webhook_twilio_sms():
    """
    Twilio inbound SMS webhook — receives text replies from pond leads.

    Twilio POSTs application/x-www-form-urlencoded to this endpoint when a
    lead replies to one of Barry's outbound AI texts.

    Flow:
      1. Validate Twilio signature (fail closed — reject if token not configured)
      2. Extract From phone + Body text
      3. Hard-gate STOP keywords before calling Claude (guarantee opt-out)
         → Return branded TwiML confirmation; log + tag SMS_OptOut; done
      4. Match phone to lead via pond_sms_log
      5. Run Claude Haiku sentiment
      6. Positive  → SMS_Conversion tag + FUB briefing note
                   → Schedule handoff SMS (4.5 min delay, agent lookup)
                   → Email Barry with conversion alert
      7. Negative  → SMS_OptOut tag (blocks future texts, leaves email untouched)
      8. Neutral   → FUB note only (reply visible in CRM timeline)
      9. Log to pond_sms_reply_log + notify Barry

    Returns TwiML <Response/> — Twilio retries on non-2xx so we always 200.
    """
    from flask import make_response as _mkr

    def _twiml(body_xml="", status=200):
        r = _mkr(f"<Response>{body_xml}</Response>", status)
        r.content_type = "text/xml"
        return r

    # ── Twilio signature validation — fail closed ─────────────────────────────
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    if not auth_token:
        # No token configured — reject everything rather than accept unauthenticated
        logger.error("TWILIO_AUTH_TOKEN not set — rejecting all inbound SMS webhooks")
        return _twiml(status=500)

    try:
        from twilio.request_validator import RequestValidator
        validator = RequestValidator(auth_token)
        sig = request.headers.get("X-Twilio-Signature", "")
        if not validator.validate(request.url, request.form.to_dict(), sig):
            logger.warning("Twilio signature validation FAILED — rejecting request")
            return _twiml(status=403)
    except ImportError:
        logger.warning("twilio package not installed — skipping signature validation")
    except Exception as _ve:
        logger.warning("Twilio signature validation error: %s — proceeding", _ve)

    try:
        from_phone = request.form.get("From", "").strip()
        body_text  = request.form.get("Body", "").strip()
        msg_sid    = request.form.get("MessageSid", "")

        logger.info("Twilio SMS reply: from=%s | body=%s", from_phone, body_text[:80])

        if not from_phone:
            logger.warning("Twilio SMS webhook: missing From field")
            return _twiml()

        # ── STOP keyword hard-gate (before Claude — no API dependency) ────────
        # CTIA standard stop words: STOP, STOPALL, UNSUBSCRIBE, CANCEL, END, QUIT
        # Check before any sentiment analysis so a STOP always applies opt-out
        # even if the Anthropic API is unavailable.
        body_lower = body_text.strip().lower()
        is_hard_stop = (
            body_lower in _SMS_HARD_STOP_WORDS or
            any(body_lower.startswith(w) for w in _SMS_HARD_STOP_WORDS)
        )

        if is_hard_stop:
            logger.info("SMS hard-stop received from %s: %r", from_phone, body_text)
            person_id_stop, person_name_stop = _db.get_pond_sms_person_by_phone(from_phone)
            if person_id_stop:
                try:
                    from fub_client import FUBClient
                    fub_stop = FUBClient()
                    fub_stop.add_tag(person_id_stop, "SMS_OptOut")
                    logger.info("SMS_OptOut tagged for hard-stop: %s (ID %s)",
                                person_name_stop, person_id_stop)
                except Exception as _se:
                    logger.warning("Could not apply SMS_OptOut for hard-stop %s: %s",
                                   from_phone, _se)
                _db.log_pond_sms_reply(
                    person_id=person_id_stop,
                    person_name=person_name_stop,
                    phone_number=from_phone,
                    reply_text=body_text[:2000],
                    sentiment="negative",
                    sentiment_score=1.0,
                    routed=False,
                    twilio_message_sid=msg_sid,
                )
            # Branded CTIA opt-out confirmation — Twilio sends this to the lead
            return _twiml(
                "<Message>You've been removed from Legacy Home Team texts. "
                "No more messages. Questions? Call (757) 919-8874.</Message>"
            )

        # Match inbound phone to a pond lead via our SMS send log
        person_id, person_name = _db.get_pond_sms_person_by_phone(from_phone)

        if not person_id:
            logger.info("Twilio SMS reply: no matching pond lead for %s", from_phone)
            return _twiml()

        logger.info("SMS reply matched: %s (ID: %s)", person_name, person_id)

        # Analyze sentiment with Claude Haiku (keyword fallback if API unavailable)
        sentiment, sentiment_score, sentiment_reason = _pond_analyze_sentiment(
            body_text, person_name
        )
        logger.info("SMS sentiment: %s (%.2f) — %s", sentiment, sentiment_score, sentiment_reason)

        routed      = False
        fub_note_ok = False

        try:
            from fub_client import FUBClient
            fub = FUBClient()

            if sentiment == "positive":
                # Same protocol as email nurture:
                #   SMS_Conversion → FUB automation assigns lead to priority group
                #   Claude_Text_Converted → visible conversion signal in FUB for Barry
                fub.add_tag(person_id, "SMS_Conversion")
                logger.info("SMS_Conversion tag applied to lead %s (%s)", person_id, person_name)
                fub.add_tag(person_id, "Claude_Text_Converted")
                logger.info("Claude_Text_Converted tag applied to lead %s (%s)", person_id, person_name)
                routed = True
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, sentiment_reason
                )
                # Schedule handoff text — wait 4.5 min for FUB automation to assign agent,
                # then send a Claude-generated message introducing their agent by name + number.
                # Pass the lead's actual reply so Claude can write something specific to them.
                _lead_first = (person_name or "").split()[0] if person_name else ""
                _schedule_sms_handoff(
                    person_id,
                    from_phone,
                    reply_text=body_text,
                    lead_first_name=_lead_first,
                    delay_seconds=270,
                )

            elif sentiment == "negative":
                fub.add_tag(person_id, "SMS_OptOut")
                logger.info("SMS_OptOut tag applied to lead %s (%s)", person_id, person_name)
            else:
                # Neutral — log note so reply is visible in CRM timeline
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, sentiment_reason
                )
        except Exception as e:
            logger.error("FUB SMS reply handling failed for %s (ID %s): %s",
                         person_name, person_id, e)

        # Log the SMS reply to DB
        _db.log_pond_sms_reply(
            person_id=person_id,
            person_name=person_name,
            phone_number=from_phone,
            reply_text=body_text[:2000],
            sentiment=sentiment,
            sentiment_score=sentiment_score,
            routed=routed,
            twilio_message_sid=msg_sid,
        )

        # ── Notify Barry ───────────────────────────────────────────────────────
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail as _SGMail
            sg_key = os.environ.get("SENDGRID_API_KEY")
            if sg_key:
                _emoji = {"positive": "🔥", "negative": "🚫", "neutral": "💬"}.get(sentiment, "📬")
                _subj  = f"{_emoji} SMS Reply from {person_name or from_phone} — {sentiment.upper()}"

                _lines = [
                    f"{person_name or 'A lead'} ({from_phone}) replied to your AI outreach text.",
                    "",
                    f"Sentiment: {sentiment.upper()} — {sentiment_reason}",
                    "",
                    "─── Their reply ───────────────────────────────────",
                    body_text.strip()[:1200],
                    "────────────────────────────────────────────────────",
                    "",
                ]
                if routed:
                    _lines.append("✅ SMS_Conversion tag applied — FUB automation is assigning now")
                    _lines.append("✅ Claude_Text_Converted tag applied — filter this in FUB")
                    _lines.append("✅ Handoff text queued — personalized agent intro sends in ~4.5 min")
                    _lines.append(f"✅ CRM note added: {'yes' if fub_note_ok else 'check logs'}")
                elif sentiment == "negative":
                    _lines.append("🚫 SMS_OptOut tag applied — lead suppressed from future texts")
                else:
                    _lines.append(f"📝 CRM note added: {'yes' if fub_note_ok else 'check logs'}")

                _lines += ["", "— Legacy Home Team AI Outreach"]
                _notify_body = "\n".join(_lines)
                _notify_html = (
                    "<div style='font-family:-apple-system,sans-serif;font-size:15px;"
                    "line-height:1.7;color:#222;max-width:560px;margin:24px auto'>"
                    + _notify_body.replace("\n", "<br>") +
                    "</div>"
                )
                _sg_msg = _SGMail(
                    from_email=config.EMAIL_FROM,
                    to_emails=config.EMAIL_FROM,
                    subject=_subj,
                    plain_text_content=_notify_body,
                    html_content=_notify_html,
                )
                SendGridAPIClient(sg_key).send(_sg_msg)
                logger.info("Barry notified of %s SMS reply from %s", sentiment, from_phone)
        except Exception as _ne:
            logger.warning("Could not notify Barry of SMS reply: %s", _ne)
        # ──────────────────────────────────────────────────────────────────────

        return _twiml()

    except Exception as e:
        logger.error("Twilio SMS webhook unhandled error: %s", e, exc_info=True)
        return _twiml()


# ── Project Blue inbound webhook ──────────────────────────────────────────────

@app.route("/webhook/projectblue", methods=["POST"])
def webhook_projectblue():
    """
    Project Blue inbound webhook — receives iMessage/SMS replies from pond leads.

    Project Blue POSTs JSON when a lead replies to one of our outbound texts.

    Payload shape (confirmed from Project Blue):
        {
          "message":         "Yes I'm interested",
          "destination":     "+15551234567",   -- the LEAD's phone (who we originally texted)
          "receivedAt":      "2026-05-09T...",
          "direction":       "inbound",
          "messageId":       456,
          "guid":            "sample-guid-1234",
          "linePhoneNumber": "+15559876543"    -- our Project Blue line
        }

    For inbound: destination = lead's phone, linePhoneNumber = our line.
    No secondary API call needed.

    Flow mirrors the Twilio handler exactly:
      1. Extract fields — destination IS the lead's phone
      2. Skip outbound confirmations (direction != "inbound")
      3. STOP keyword hard-gate
      4. Match phone to FUB lead via pond_sms_log
      5. Sentiment analysis (Claude Haiku)
      6. Positive  -> SMS_Conversion + FUB note + handoff text
      7. Negative  -> SMS_OptOut
      8. Neutral   -> FUB note
      9. Log to pond_sms_reply_log + email Barry
    """
    try:
        data = request.get_json(force=True, silent=True) or {}

        direction = data.get("direction", "")
        if direction != "inbound":
            # PB fires webhooks for outbound confirmations too — ignore them
            return jsonify({"ok": True, "skipped": "outbound"}), 200

        body_text  = (data.get("message") or "").strip()
        guid       = data.get("guid", "")
        from_phone = (data.get("destination") or "").strip()   # lead's phone
        our_line   = (data.get("linePhoneNumber") or "").strip()

        logger.info("Project Blue inbound: from=%s line=%s guid=%s body=%s",
                    from_phone, our_line, guid, body_text[:80])

        if not from_phone:
            logger.warning("Project Blue webhook: missing destination (lead phone)")
            return jsonify({"ok": True, "skipped": "no_from_phone"}), 200

        if not body_text:
            logger.warning("Project Blue webhook: empty body from %s", from_phone)
            return jsonify({"ok": True}), 200

        # ── STOP keyword hard-gate ─────────────────────────────────────────────
        body_lower = body_text.lower().strip()
        is_hard_stop = (
            body_lower in _SMS_HARD_STOP_WORDS or
            any(body_lower.startswith(w) for w in _SMS_HARD_STOP_WORDS)
        )

        if is_hard_stop:
            logger.info("PB hard-stop from %s: %r", from_phone, body_text)
            person_id_stop, person_name_stop = _db.get_pond_sms_person_by_phone(from_phone)
            if person_id_stop:
                try:
                    from fub_client import FUBClient
                    fub_stop = FUBClient()
                    fub_stop.add_tag(person_id_stop, "SMS_OptOut")
                except Exception as _se:
                    logger.warning("SMS_OptOut tag failed for PB stop %s: %s", from_phone, _se)
                _db.log_pond_sms_reply(
                    person_id=person_id_stop,
                    person_name=person_name_stop,
                    phone_number=from_phone,
                    reply_text=body_text[:2000],
                    sentiment="negative",
                    sentiment_score=1.0,
                    routed=False,
                    twilio_message_sid=guid,
                )
            return jsonify({"ok": True, "action": "opted_out"}), 200

        # ── Match phone to FUB lead ────────────────────────────────────────────
        person_id, person_name = _db.get_pond_sms_person_by_phone(from_phone)

        if not person_id:
            logger.info("PB inbound: no matching pond lead for %s", from_phone)
            return jsonify({"ok": True, "skipped": "no_match"}), 200

        logger.info("PB SMS reply matched: %s (ID: %s)", person_name, person_id)

        # ── Sentiment ──────────────────────────────────────────────────────────
        sentiment, sentiment_score, sentiment_reason = _pond_analyze_sentiment(
            body_text, person_name
        )
        logger.info("PB SMS sentiment: %s (%.2f) — %s", sentiment, sentiment_score, sentiment_reason)

        routed      = False
        fub_note_ok = False

        # Consent detection runs outside the FUB try/except so it's always
        # defined and can be safely referenced in the notify-Barry block below.
        _consent = _is_consent_reply(body_text)

        # Audit tracking — captured inside consent block, read by audit email below
        _audit_voice_script = None
        _audit_video_id     = None
        _audit_video_url    = None
        _audit_handoff_secs = None
        _audit_behavior     = {}
        _audit_lead_type    = "buyer"  # refined below when we fetch person record

        try:
            from fub_client import FUBClient
            fub = FUBClient()
            if _consent:
                logger.info("Consent reply detected from %s (%s): %r",
                            person_name, from_phone, body_text[:60])
                try:
                    import elevenlabs_client as _el
                    import projectblue_client as _pb_reply

                    # Look up the A/B variant assigned when we sent the opener
                    _ab_variant = _db.get_ab_variant_for_lead(person_id) or "voice"
                    logger.info("A/B variant for %s: %s", person_name, _ab_variant)

                    _base_url = os.environ.get("BASE_URL",
                                               "https://web-production-3363cc.up.railway.app")

                    if _ab_variant == "video":
                        # Video variant: HeyGen thumbnail + link as MMS
                        _stored_vid_id = _db.get_video_id_for_lead(person_id)
                        if _stored_vid_id and _pb_reply.is_available():
                            _thumb_url = f"{_base_url}/mthumb/{_stored_vid_id}"
                            _vid_link  = f"{_base_url}/v/{_stored_vid_id}"
                            _vid_body  = (
                                f"here you go! tap the link for the full walkthrough\n"
                                f"{_vid_link}"
                            )
                            _pb_reply.send_message(
                                to_number=from_phone,
                                body=_vid_body,
                                media_url=_thumb_url,
                            )
                            _audit_video_id  = _stored_vid_id
                            _audit_video_url = _vid_link
                            logger.info("Video variant sent to %s (video_id=%s)",
                                        person_name, _stored_vid_id)
                        else:
                            logger.warning("Video variant: no video_id for %s — falling back to voice",
                                           person_name)
                            _ab_variant = "voice"   # fall through to voice path

                    if _ab_variant != "video":
                        # Voice variant: ElevenLabs audio bubble in iMessage
                        if _el.is_available() and _pb_reply.is_available():
                            _vn_behavior = {}
                            try:
                                from fub_client import FUBClient as _FUBv
                                from pond_mailer import analyze_behavior as _ab_fn
                                _vn_fub    = _FUBv()
                                _vn_events = _vn_fub.get_events_for_person(
                                    person_id, days=90, limit=100)
                                _vn_tags   = fub.get_person(person_id).get("tags", [])
                                _vn_behavior = _ab_fn(_vn_events, _vn_tags)
                            except Exception as _vbe:
                                logger.warning("Voice note behavior fetch failed: %s", _vbe)

                            # Lead type detection for voice note script routing.
                            # Zbuyer: owns the home, wants a cash offer — totally different script.
                            # Ylopo Prospecting/Seller: home value inquiry — market intel script.
                            # Everything else: buyer browsing IDX.
                            _vn_person_full = fub.get_person(person_id) if person_id else {}
                            _vn_person_tags = _vn_person_full.get("tags") or []
                            _vn_source = (_vn_person_full.get("source") or "").strip().lower()
                            _vn_src_norm = _vn_source.replace("-","").replace(" ","").replace("_","")
                            _is_zbuyer_vn = (
                                any(t.upper().replace("-","_") in ("ZLEAD","Z_BUYER","YLOPO_Z_BUYER")
                                    for t in _vn_person_tags)
                                or "zbuyer" in _vn_src_norm
                            )
                            _is_seller_vn = (
                                not _is_zbuyer_vn
                                and any(t in _vn_person_tags
                                        for t in ("Ylopo Prospecting", "Ylopo Seller"))
                            )

                            # Capture lead type for audit email
                            _audit_lead_type = (
                                "zbuyer" if _is_zbuyer_vn else
                                ("seller" if _is_seller_vn else "buyer")
                            )
                            _audit_behavior = _vn_behavior

                            script      = _el.generate_voice_note_script(
                                person_name=person_name,
                                behavior=_vn_behavior,
                                strategy="",
                                is_seller=_is_seller_vn,
                                is_zbuyer=_is_zbuyer_vn,
                            )
                            _audit_voice_script = script   # capture for audit email
                            audio_bytes = _el.generate_audio(script)
                            if audio_bytes:
                                audio_id  = _el.store_audio(audio_bytes)
                                audio_url = f"{_base_url}/audio/{audio_id}"
                                _pb_reply.send_message(
                                    to_number=from_phone,
                                    body="",
                                    audio_url=audio_url,
                                )
                                logger.info("Voice note sent to %s (%d bytes, %d chars)",
                                            from_phone, len(audio_bytes), len(script))
                            else:
                                logger.warning("ElevenLabs returned no audio for %s", person_name)
                        else:
                            logger.info("Voice note skipped — ElevenLabs or Project Blue not configured")
                except Exception as _vne:
                    logger.warning("Recording send failed (non-fatal): %s", _vne)

            _lead_first = (person_name or "").split()[0] if person_name else ""

            if sentiment == "positive":
                fub.add_tag(person_id, "SMS_Conversion")
                fub.add_tag(person_id, "Claude_Text_Converted")
                routed = True
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, sentiment_reason
                )
                # 270s: give FUB automation time to assign the lead before handoff fires
                _audit_handoff_secs = 270
                _schedule_sms_handoff(
                    person_id,
                    from_phone,
                    reply_text=body_text,
                    lead_first_name=_lead_first,
                    delay_seconds=270,
                )
            elif sentiment == "negative":
                fub.add_tag(person_id, "SMS_OptOut")
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, sentiment_reason
                )
            elif _consent:
                # Consent with neutral buying intent: they said yes to the recording
                # but didn't show buying signals yet. Log the FUB note AND schedule
                # a soft follow-up 15 min later so an agent can check in after they've
                # had time to listen to the voice note.
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, "consented to recording"
                )
                _audit_handoff_secs = 900
                _schedule_sms_handoff(
                    person_id,
                    from_phone,
                    reply_text=body_text,
                    lead_first_name=_lead_first,
                    delay_seconds=900,   # 15 min — time to listen to the note first
                )
            else:
                fub_note_ok = _pond_add_sms_reply_fub_note(
                    fub, person_id, person_name, body_text, sentiment_reason
                )
        except Exception as e:
            logger.error("FUB PB reply handling failed for %s (ID %s): %s",
                         person_name, person_id, e)

        # ── Log to DB ──────────────────────────────────────────────────────────
        _db.log_pond_sms_reply(
            person_id=person_id,
            person_name=person_name,
            phone_number=from_phone,
            reply_text=body_text[:2000],
            sentiment=sentiment,
            sentiment_score=sentiment_score,
            routed=routed,
            twilio_message_sid=guid,   # reusing column for PB guid
        )

        # ── Lead audit email — full breakdown so Barry can QA every touch ──────
        try:
            import lead_audit as _la
            _orig_sms = None
            try:
                _orig_sms = _db.get_last_sms_sent(person_id)
            except Exception:
                pass
            _la.send_response_audit(
                person_id=person_id,
                person_name=person_name or from_phone,
                lead_type=_audit_lead_type,
                phone=from_phone,
                reply_text=body_text,
                sentiment=sentiment,
                sentiment_reason=sentiment_reason,
                consent=_consent,
                ab_variant=_db.get_ab_variant_for_lead(person_id) or "voice",
                voice_script=_audit_voice_script,
                video_id=_audit_video_id,
                video_url=_audit_video_url,
                handoff_delay_seconds=_audit_handoff_secs,
                original_sms=_orig_sms,
                behavior=_audit_behavior,
            )
        except Exception as _lae:
            logger.warning("Lead audit email (response) failed: %s", _lae)

        # ── Notify Barry ───────────────────────────────────────────────────────
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail as _SGMail
            sg_key = os.environ.get("SENDGRID_API_KEY")
            if sg_key:
                _emoji = {"positive": "🔥", "negative": "🚫", "neutral": "💬"}.get(sentiment, "📬")
                if _consent and sentiment != "negative":
                    _emoji = "🎙️" if (_db.get_ab_variant_for_lead(person_id) or "voice") == "voice" else "🎬"
                _subj  = f"{_emoji} iMessage Reply from {person_name or from_phone} — {sentiment.upper()}"
                _lines = [
                    f"{person_name or 'A lead'} ({from_phone}) replied to your AI iMessage.",
                    "",
                    f"Sentiment: {sentiment.upper()} — {sentiment_reason}",
                ]
                if _consent:
                    _resolved_variant = _db.get_ab_variant_for_lead(person_id) or "voice"
                    _lines.append(f"Consent: YES — {_resolved_variant} recording sent")
                _lines += [
                    "",
                    "─── Their reply ────────────────────────────────────",
                    body_text.strip()[:1200],
                    "────────────────────────────────────────────────────",
                    "",
                ]
                if routed:
                    _lines.append("SMS_Conversion tag applied")
                    _lines.append("Handoff text queued (4.5 min)")
                    _lines.append(f"CRM note added: {'yes' if fub_note_ok else 'check logs'}")
                elif sentiment == "negative":
                    _lines.append("SMS_OptOut tag applied")
                else:
                    _lines.append(f"CRM note added: {'yes' if fub_note_ok else 'check logs'}")
                _lines += ["", "— Legacy Home Team AI Outreach"]

                _notify_body = "\n".join(_lines)
                _notify_html = (
                    "<div style='font-family:-apple-system,sans-serif;font-size:15px;"
                    "line-height:1.7;color:#222;max-width:560px;margin:24px auto'>"
                    + _notify_body.replace("\n", "<br>") + "</div>"
                )
                _sg_msg = _SGMail(
                    from_email=config.EMAIL_FROM,
                    to_emails=config.EMAIL_FROM,
                    subject=_subj,
                    plain_text_content=_notify_body,
                    html_content=_notify_html,
                )
                SendGridAPIClient(sg_key).send(_sg_msg)
        except Exception as _ne:
            logger.warning("Barry notify email failed for PB reply: %s", _ne)

        return jsonify({"ok": True}), 200

    except Exception as e:
        logger.error("Project Blue webhook unhandled error: %s", e, exc_info=True)
        return jsonify({"ok": True, "error": "internal"}), 200


@app.route("/api/pond-mailer/dashboard")
def api_pond_mailer_dashboard():
    """
    Full AI Outreach dashboard data.
    Pulls funnel stats + routed leads from DB, then enriches each routed lead
    with live FUB data: current stage, assigned agent name, and call count
    since the reply date so Barry can see who followed up and who dropped the ball.
    Cached 5 minutes — enrichment makes ~2 FUB API calls per routed lead.
    """
    cache_key = "pond_mailer_dashboard"
    cached = _cache.get(cache_key)
    if cached and (datetime.now().timestamp() - cached["ts"]) < 300:
        return jsonify(cached["data"])

    try:
        from fub_client import FUBClient

        base = _db.get_pond_dashboard_data(days=30)
        if not base:
            return jsonify({"error": "Database not available"}), 503

        fub = FUBClient()

        # Enrich each routed lead with live FUB data
        enriched = []
        for lead in base.get("routed_leads", []):
            pid          = lead["person_id"]
            routing_ts   = lead.get("received_ts", 0)
            routing_dt   = datetime.fromtimestamp(routing_ts, tz=timezone.utc) if routing_ts else None

            agent_name   = "Unassigned"
            stage        = "Unknown"
            calls_since  = 0
            last_call_dt = None

            try:
                person = fub.get_person(pid)
                stage  = person.get("stage") or person.get("stageName") or "Unknown"

                # Assigned agent name
                uid = person.get("assignedUserId") or person.get("ownerId")
                if uid:
                    # Try agent_profiles table first (fast)
                    from db import get_conn as _get_conn, is_available as _db_avail
                    if _db_avail():
                        try:
                            with _get_conn() as conn:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        "SELECT agent_name FROM agent_profiles WHERE fub_user_id=%s LIMIT 1",
                                        (uid,)
                                    )
                                    row = cur.fetchone()
                            if row:
                                agent_name = row[0]
                        except Exception:
                            pass
                    if agent_name == "Unassigned":
                        # Fall back to FUB user lookup
                        try:
                            user = fub._request("GET", f"users/{uid}")
                            agent_name = (
                                f"{user.get('firstName','')} {user.get('lastName','')}".strip()
                                or f"User {uid}"
                            )
                        except Exception:
                            agent_name = f"Agent #{uid}"

                # Calls since routing date
                if routing_dt:
                    calls = fub.get_calls(person_id=pid, since=routing_dt)
                    calls_since = len(calls)
                    if calls:
                        last_call_dt = max(
                            (c.get("created", "") for c in calls),
                            default=None
                        )

            except Exception as e:
                logger.warning("FUB enrichment failed for lead %s: %s", pid, e)

            # Status: green=called, yellow=task pending, red=dropped
            hours_since = (
                (datetime.now(timezone.utc) - routing_dt).total_seconds() / 3600
                if routing_dt else 0
            )
            if calls_since > 0:
                status = "contacted"
            elif hours_since < 24:
                status = "pending"
            else:
                status = "dropped"

            enriched.append({
                **lead,
                "agent_name":   agent_name,
                "stage":        stage,
                "calls_since":  calls_since,
                "last_call_dt": last_call_dt,
                "hours_since":  round(hours_since, 1),
                "status":       status,
            })

        result = {**base, "routed_leads": enriched}
        _cache[cache_key] = {"ts": datetime.now().timestamp(), "data": result}
        return jsonify(result)

    except Exception as e:
        logger.error("Pond mailer dashboard error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/pond-mailer/replies")
def api_pond_mailer_reply_stats():
    """Reply stats — how many leads replied, sentiment breakdown, routing rate."""
    try:
        stats = _db.get_pond_reply_stats(days=30)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test-mapbox", methods=["GET", "POST"])
def api_test_mapbox():
    """
    Quick test: geocode an address → build Mapbox map URL → optionally submit
    a HeyGen test video (non-blocking — returns video_id immediately).

    GET/POST params:
        address  — street address (default: "5308 Summer Crescent")
        city     — city (default: "Virginia Beach")
        video    — "true" to also submit a HeyGen test video
    """
    import threading
    from flask import request
    try:
        from heygen_client import get_background_url, submit_video
        address = request.values.get("address", "5308 Summer Crescent")
        city    = request.values.get("city", "Virginia Beach")
        do_vid  = request.values.get("video", "false").lower() == "true"

        bg_url = get_background_url("seller", address=address, city=city)
        if not bg_url:
            return jsonify({"error": "Mapbox geocoding failed — check MAPBOX_ACCESS_TOKEN"}), 400

        result = {
            "map_url":  bg_url,
            "address":  f"{address}, {city}, VA",
            "map_style": "navigation-day-v1 (GPS road-map style)",
            "preview":  f"Open this URL in your browser to preview the map image: {bg_url}",
        }

        if do_vid:
            script = (
                f"Hi, I'm Barry Jenkins with Legacy Home Team. "
                f"I wanted to personally reach out about the real estate market near {address} in {city}. "
                f"The market is moving fast right now — if you'd like a quick update on your area, "
                f"just reply and I'll get you the numbers."
            )
            # Submit async — HeyGen takes 1-3 minutes to render; return ID immediately
            video_id = submit_video(
                script=script,
                background_url=bg_url,
                avatar_style="circle",
                title=f"Map Test — {address}",
            )
            if video_id:
                result["video_id"]     = video_id
                result["video_status"] = "processing"
                result["check_status"] = f"curl https://web-production-3363cc.up.railway.app/api/test-heygen-status/{video_id}"
            else:
                result["video_error"] = "HeyGen video submission failed — check HEYGEN_API_KEY"

        return jsonify(result)
    except Exception as e:
        logger.exception("api_test_mapbox failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/test-heygen-status/<video_id>")
def api_test_heygen_status(video_id: str):
    """Poll HeyGen video status by ID."""
    try:
        import requests as _req
        import os
        api_key = os.environ.get("HEYGEN_API_KEY", "")
        r = _req.get(
            "https://api.heygen.com/v1/video_status.get",
            params={"video_id": video_id},
            headers={"X-Api-Key": api_key},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json().get("data", {})
        return jsonify({
            "video_id":  video_id,
            "status":    data.get("status"),
            "video_url": data.get("video_url"),
            "thumbnail": data.get("thumbnail_url"),
            "error":     data.get("error"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pond-mailer/diagnose-heygen")
def api_diagnose_heygen():
    """
    Step-by-step HeyGen diagnostic. Picks the first eligible pond lead and
    walks through every gate: API key → lead → sequence → script → submit → poll.
    Returns a JSON log showing exactly which step succeeded or failed.
    Safe — dry-run only, never sends anything.
    """
    import time as _time
    steps = []

    def step(name, ok, detail=""):
        steps.append({"step": name, "ok": ok, "detail": str(detail)[:400]})

    try:
        # Step 1: Check API key
        heygen_key = os.environ.get("HEYGEN_API_KEY", "")
        step("HEYGEN_API_KEY set", bool(heygen_key),
             "Set on Railway" if heygen_key else "MISSING — set this env var on Railway")
        if not heygen_key:
            return jsonify({"steps": steps, "verdict": "HEYGEN_API_KEY not set"})

        # Step 2: Check FUB key + find a pond lead
        from fub_client import FUBClient
        from config import LEADSTREAM_ALLOWED_POND_IDS
        client = FUBClient()
        pond_lead = None
        for pond_id in sorted(LEADSTREAM_ALLOWED_POND_IDS):
            leads = client.get_people_recent(pond_id=pond_id, limit=20)
            for p in leads:
                emails = p.get("emails") or []
                has_email = any(e.get("value") for e in emails)
                if has_email:
                    pond_lead = p
                    break
            if pond_lead:
                break

        step("Found pond lead with email", bool(pond_lead),
             f"FUB person ID {pond_lead.get('id')}: {pond_lead.get('firstName')} {pond_lead.get('lastName')}"
             if pond_lead else f"No leads with email in ponds {sorted(LEADSTREAM_ALLOWED_POND_IDS)}")
        if not pond_lead:
            return jsonify({"steps": steps, "verdict": "No eligible pond leads found"})

        pid   = pond_lead.get("id")
        first = pond_lead.get("firstName") or "there"

        # Step 3: Sequence check
        import db as _db_local
        history = _db_local.get_lead_email_history(pid)
        seq_num = history.get("sequence_num", 1)
        suppressed = history.get("suppressed", False)
        has_replied = history.get("has_replied", False)
        step("Not suppressed / replied", not suppressed and not has_replied,
             f"sequence_num={seq_num}, suppressed={suppressed}, has_replied={has_replied}")

        step("Eligible for Email 1 HeyGen (sequence_num == 1)", seq_num == 1,
             f"sequence_num={seq_num} — HeyGen only fires on Email 1. "
             f"{'Lead already got email 1+ without a video, now on text drip.' if seq_num > 1 else 'Ready for Email 1 video.'}")

        if seq_num != 1:
            # Try to find a true sequence-1 lead
            for pond_id in sorted(LEADSTREAM_ALLOWED_POND_IDS):
                leads = client.get_people_recent(pond_id=pond_id, limit=50)
                for p in leads:
                    emails = p.get("emails") or []
                    if not any(e.get("value") for e in emails):
                        continue
                    h2 = _db_local.get_lead_email_history(p.get("id"))
                    if h2.get("sequence_num", 1) == 1 and not h2.get("suppressed"):
                        pond_lead = p
                        pid   = p.get("id")
                        first = p.get("firstName") or "there"
                        seq_num = 1
                        break
                if seq_num == 1:
                    break
            step("Found sequence-1 lead for full test", seq_num == 1,
                 f"Using {pond_lead.get('firstName')} {pond_lead.get('lastName')} (ID {pid})"
                 if seq_num == 1 else "All pond leads already have email 1 — HeyGen won't fire until new leads come in")

        # Step 4: HeyGen daily cap check
        hg_cap = 8
        hg_used = _db_local.count_heygen_today()
        slots_left = max(0, hg_cap - hg_used)
        step("Under HeyGen daily cap (8/day)", slots_left > 0,
             f"{hg_used} used today, {slots_left} slots remaining (cap={hg_cap})")

        # Step 5: HEYGEN_API_KEY actually works — ping the API
        import requests as _req
        try:
            r_ping = _req.get(
                "https://api.heygen.com/v1/user/remaining_quota",
                headers={"X-Api-Key": heygen_key},
                timeout=10,
            )
            if r_ping.status_code == 200:
                quota = r_ping.json().get("data", {})
                credits = quota.get("remaining_credits") or quota.get("credit_remaining")
                step("HeyGen API key valid", True,
                     f"API responded 200. Credits remaining: {credits}")
            else:
                step("HeyGen API key valid", False,
                     f"API returned {r_ping.status_code}: {r_ping.text[:300]}")
                return jsonify({"steps": steps,
                                "verdict": f"HeyGen API key rejected — HTTP {r_ping.status_code}"})
        except Exception as ping_err:
            step("HeyGen API reachable", False, str(ping_err))
            return jsonify({"steps": steps, "verdict": "Cannot reach HeyGen API"})

        # Step 6: Generate a buyer video script (uses Claude)
        try:
            from heygen_client import generate_buyer_video_script, DEFAULT_AVATAR, DEFAULT_AVATAR_TYPE, DEFAULT_VOICE, get_background_url
            tags  = pond_lead.get("tags") or []
            script = generate_buyer_video_script(
                first_name=first, city="Virginia Beach",
                strategy="any_activity", view_count=2, tags=tags,
            )
            step("Video script generated (Claude)", bool(script),
                 f"{len(script)} chars: \"{script[:120]}…\"")
        except Exception as script_err:
            step("Video script generated (Claude)", False, str(script_err))
            return jsonify({"steps": steps, "verdict": f"Script generation failed: {script_err}"})

        # Step 7: Submit to HeyGen
        try:
            from heygen_client import submit_video
            bg_url = get_background_url("buyer", city="Virginia Beach")
            step("Mapbox background URL", bool(bg_url),
                 bg_url if bg_url else "MAPBOX_ACCESS_TOKEN not set — using color fallback (OK)")
            t0 = _time.time()
            video_id = submit_video(script, background_url=bg_url,
                                    avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE,
                                    character_type=DEFAULT_AVATAR_TYPE,
                                    title=f"Diagnostic test — {first}")
            elapsed = _time.time() - t0
            step("HeyGen video submitted", bool(video_id),
                 f"video_id={video_id} ({elapsed:.1f}s)" if video_id else
                 "submit_video returned None — check HEYGEN_API_KEY or quota")
        except Exception as submit_err:
            step("HeyGen video submitted", False, str(submit_err))
            return jsonify({"steps": steps, "verdict": f"HeyGen submit failed: {submit_err}"})

        if not video_id:
            return jsonify({
                "steps": steps,
                "verdict": "HeyGen submit failed — likely bad API key, exhausted credits, or invalid avatar/voice ID",
                "fix": "Check HEYGEN_API_KEY env var on Railway. Verify account credits at app.heygen.com."
            })

        # Step 8: Poll briefly (30s) to confirm rendering starts
        step("Polling for render status (30s max)…", True, "Checking every 5 seconds")
        poll_status = None
        poll_detail = ""
        for _ in range(6):
            _time.sleep(5)
            try:
                rp = _req.get("https://api.heygen.com/v1/video_status.get",
                              headers={"X-Api-Key": heygen_key},
                              params={"video_id": video_id}, timeout=10)
                if rp.status_code == 200:
                    data = rp.json().get("data", {})
                    poll_status = data.get("status")
                    poll_detail = f"status={poll_status}"
                    if poll_status in ("completed", "failed"):
                        break
            except Exception:
                pass

        render_ok = poll_status in ("completed", "processing", "pending", "waiting")
        step("Render started / completed", render_ok,
             poll_detail or "No status returned")

        verdict = (
            "✓ HeyGen pipeline is fully working. Videos will go out on the next email-1 lead."
            if render_ok and seq_num == 1
            else "✓ HeyGen API works! But all current pond leads are on email 2+. "
                 "New leads will get video on email 1."
            if render_ok
            else f"✗ HeyGen render issue: {poll_detail}"
        )
        return jsonify({"steps": steps, "verdict": verdict, "test_video_id": video_id})

    except Exception as e:
        import traceback
        step("Unexpected error", False, traceback.format_exc()[:500])
        return jsonify({"steps": steps, "verdict": f"Unexpected error: {e}"})


@app.route("/api/pond-mailer/test-heygen-email")
def api_test_heygen_email():
    """
    Generate a talking-photo test video and email it to Barry.

    Submit step is synchronous — returns immediately with the HeyGen response
    (success or full error body) so failures are visible in the admin page.
    Polling + email delivery run in a background thread.
    """
    import threading, time as _time, logging as _logging, os as _os, requests as _req
    _log = _logging.getLogger(__name__)

    from heygen_client import (
        get_background_url, poll_video,
        make_video_email_html, make_video_plain_text,
        DEFAULT_AVATAR, DEFAULT_AVATAR_TYPE, DEFAULT_VOICE,
        AVATAR_CIRCLE_SCALE, AVATAR_CIRCLE_OFFSET_X, AVATAR_CIRCLE_OFFSET_Y,
    )
    import config

    heygen_key = _os.environ.get("HEYGEN_API_KEY", "")
    if not heygen_key:
        return jsonify({"ok": False, "error": "HEYGEN_API_KEY not set on Railway"})

    test_script = (
        "Hey Barry — this is a quick test of the new talking photo avatar. "
        "I'm floating right on top of the Hampton Roads map. "
        "No blue circle, just my face. "
        "Let me know if this looks right and we're good to go."
    )

    bg_url = get_background_url("buyer", city="Virginia Beach")

    # Build the talking-photo character block
    character = {
        "type":             "talking_photo",
        "talking_photo_id": DEFAULT_AVATAR,
        "scale":  AVATAR_CIRCLE_SCALE,
        "offset": {"x": AVATAR_CIRCLE_OFFSET_X, "y": AVATAR_CIRCLE_OFFSET_Y},
    }
    payload = {
        "video_inputs": [{
            "character": character,
            "voice": {
                "type":       "text",
                "voice_id":   DEFAULT_VOICE,
                "input_text": test_script,
                "speed":      0.9,
            },
            "background": {"type": "image", "url": bg_url} if bg_url
                          else {"type": "color", "value": "#0c1228"},
        }],
        "dimension": {"width": 1920, "height": 1080},
        "title":   "Barry — talking photo test",
        "quality": "medium",
    }

    # ── Submit synchronously so errors surface immediately ────────────────────
    try:
        r = _req.post("https://api.heygen.com/v2/video/generate",
                      headers={"X-Api-Key": heygen_key, "Content-Type": "application/json"},
                      json=payload, timeout=15)
        resp_body = r.json() if r.headers.get("content-type","").startswith("application/json") else r.text
    except Exception as submit_err:
        return jsonify({"ok": False, "error": f"HeyGen request failed: {submit_err}"})

    if r.status_code != 200:
        return jsonify({
            "ok":    False,
            "error": f"HeyGen returned HTTP {r.status_code}",
            "body":  resp_body,
            "payload_sent": payload,   # show exactly what we sent for debugging
        })

    video_id = (resp_body.get("data") or {}).get("video_id")
    if not video_id:
        return jsonify({"ok": False, "error": "No video_id in response", "body": resp_body})

    _log.info("Test email: video submitted %s — polling in background", video_id)

    # ── Poll + send email in background ──────────────────────────────────────
    def _finish(vid_id, bg):
        try:
            result = poll_video(vid_id, timeout_seconds=480, poll_interval=8)
            if not result or not result.get("video_url"):
                _log.error("Test email: video timed out or failed — %s", result)
                return
            video_url     = result["video_url"]
            thumbnail_url = result.get("thumbnail_url", "")
            duration      = result.get("duration", 0)

            body_html = make_video_email_html(
                setup_text="Barry — here's the talking photo test on the Hampton Roads map.",
                video_url=video_url, thumbnail_url=thumbnail_url,
                cta_text="Reply if the face cutout looks right and we're ready to ship.",
                first_name="Barry", caption="talking photo test · no blue circle",
                duration=duration, video_id=vid_id, map_url=bg or "",
            )
            body_text = (
                "Barry — talking photo test on the Hampton Roads map.\n\n"
                + make_video_plain_text(video_url, first_name="Barry", video_id=vid_id)
                + "\nReply if the face cutout looks right and we're ready to ship.\n\n"
                  "— Barry Jenkins AI Nurture · Legacy Home Team"
            )

            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Email, To, Content
            sg_key = _os.environ.get("SENDGRID_API_KEY", "")
            if not sg_key:
                _log.error("Test email: SENDGRID_API_KEY not set")
                return
            msg = Mail(from_email=Email(config.EMAIL_FROM, "Barry Jenkins"),
                       to_emails=To(config.EMAIL_FROM),
                       subject="[Test] Talking photo on map — does this look right?")
            msg.add_content(Content("text/plain", body_text))
            msg.add_content(Content("text/html",  body_html))
            sg = SendGridAPIClient(sg_key)
            resp = sg.client.mail.send.post(request_body=msg.get())
            _log.info("Test email sent → %d", resp.status_code)
        except Exception as _err:
            import traceback as _tb
            _log.error("Test email finish failed: %s\n%s", _err, _tb.format_exc())

    threading.Thread(target=_finish, args=(video_id, bg_url), daemon=True).start()

    return jsonify({
        "ok":      True,
        "video_id": video_id,
        "message": f"✓ HeyGen accepted the video (id: {video_id}). "
                   f"Rendering now — email arrives in ~3-5 min at {config.EMAIL_FROM}",
    })


@app.route("/api/test-sms", methods=["POST"])
def api_test_sms():
    """
    Send a test SMS or MMS via Twilio. Admin/dev use only.
    Body: {
        "to":                  "+1...",
        "body":                "...",
        "media_url":           "https://..." (optional),
        "video_id":            "32-char HeyGen ID" (optional — auto-builds mthumb + short link),
        "bypass_quiet_hours":  true           (optional — for after-hours testing to Barry's number)
    }
    When video_id is provided: attaches /mthumb/<id> as MMS image and appends
    the /go/<code> short URL to the body automatically — no manual URL needed.
    """
    from twilio_client import send_sms as _send_sms, format_e164, is_available, SMS_SIGN_OFF
    data               = request.json or {}
    to                 = data.get("to", "+17578164037")
    body               = data.get("body", "Test from Legacy Home Team system.")
    media_url          = data.get("media_url")
    video_id           = (data.get("video_id") or "").strip().lower()
    bypass_quiet_hours = bool(data.get("bypass_quiet_hours", False))

    # If video_id provided, auto-build mthumb URL + register short link + append to body
    if video_id:
        _base = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")
        if not media_url:
            media_url = f"{_base}/mthumb/{video_id}"
        short_link = make_short_video_url(video_id)
        if short_link not in body:
            body = f"{body}\n\n{short_link}"

    # Standard path — respects TCPA quiet hours
    if not bypass_quiet_hours:
        result = _send_sms(to, body, media_url=media_url, dry_run=False)
        return jsonify(result)

    # Bypass path — direct Twilio call, skips quiet-hours gate.
    # Only safe for internal test numbers (Barry's own phone).
    if not is_available():
        return jsonify({"success": False, "status": "failed", "error": "Twilio not configured"})
    try:
        from twilio.rest import Client as _TwilioClient
        _client = _TwilioClient(os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"])
        _full_body  = f"{body}\n{SMS_SIGN_OFF}"
        _kwargs     = {
            "messaging_service_sid": os.environ["TWILIO_MESSAGING_SERVICE_SID"],
            "to":   format_e164(to),
            "body": _full_body,
        }
        if media_url:
            _kwargs["media_url"] = [media_url]
        _msg = _client.messages.create(**_kwargs)
        return jsonify({"success": True, "twilio_sid": _msg.sid, "status": _msg.status})
    except Exception as _e:
        return jsonify({"success": False, "status": "failed", "error": str(_e)})


@app.route("/api/twilio-status/<sid>", methods=["GET"])
def api_twilio_status(sid):
    """Look up a Twilio message SID and return its current delivery status."""
    from twilio_client import is_available
    if not is_available():
        return jsonify({"error": "Twilio not configured"}), 400
    try:
        from twilio.rest import Client as _TC
        _client = _TC(os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"])
        msg = _client.messages(sid).fetch()
        return jsonify({
            "sid":          msg.sid,
            "status":       msg.status,
            "error_code":   msg.error_code,
            "error_message": msg.error_message,
            "to":           msg.to,
            "num_media":    msg.num_media,
            "direction":    msg.direction,
            "date_sent":    str(msg.date_sent),
        })
    except Exception as _e:
        return jsonify({"error": str(_e)}), 500


# ---- Scheduler: cache warming + email delivery ----
_scheduler_started = False
_scheduler = None          # global ref so health endpoint can inspect jobs
_job_last_fired = {}       # job_id -> ISO timestamp of last successful fire
_COOLDOWN_FILE = "/tmp/fub_email_fired.json"  # persists across process restarts


def _load_cooldown_state():
    """Load persisted email cooldown state from disk.

    /tmp survives gunicorn worker restarts and Railway process crashes.
    It is only cleared on new deploys — which is acceptable because a fresh
    deploy means intentionally new code, not an accidental double-send.
    """
    global _job_last_fired
    try:
        if os.path.exists(_COOLDOWN_FILE):
            with open(_COOLDOWN_FILE) as f:
                loaded = json.load(f)
            _job_last_fired.update(loaded)
            print(f"[SCHEDULER] Loaded cooldown state: {loaded}")
    except Exception as e:
        print(f"[SCHEDULER] Could not load cooldown state: {e}")


def warmup_cache():
    """Pre-populate all 3 tab caches so page loads are instant."""
    import threading
    def _warmup():
        try:
            with app.test_client() as tc:
                print("[WARMUP] Pre-loading audit data...")
                tc.get("/api/audit")
                print("[WARMUP] Audit cached ✓")
                tc.get("/api/manager")
                print("[WARMUP] Manager cached ✓")
                tc.get("/api/isa")
                print("[WARMUP] ISA cached ✓")
                tc.get("/api/appointments")
                print("[WARMUP] Appointments cached ✓")
        except Exception as e:
            print(f"[WARMUP] Error: {e}")
    t = threading.Thread(target=_warmup, daemon=True)
    t.start()


def _record_fired(job_id):
    _job_last_fired[job_id] = datetime.now(timezone.utc).isoformat()
    try:
        with open(_COOLDOWN_FILE, "w") as f:
            json.dump(_job_last_fired, f)
    except Exception as e:
        print(f"[SCHEDULER] Could not persist cooldown state: {e}")


def _already_fired_recently(job_id, within_hours=4):
    """Return True if job fired within the last N hours (duplicate guard for emails)."""
    last = _job_last_fired.get(job_id)
    if not last:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now(timezone.utc) - last_dt).total_seconds() < within_hours * 3600
    except Exception:
        return False


def _alert_on_job_failure(job_name: str, error: str):
    """Send a quick email to Barry when a scheduled job throws an unhandled exception."""
    try:
        api_key = os.environ.get("SENDGRID_API_KEY")
        if not api_key:
            return
        import sendgrid as _sg
        from sendgrid.helpers.mail import Mail as _Mail
        msg = _Mail(
            from_email=config.EMAIL_FROM,
            to_emails=config.EMAIL_FROM,  # Barry's email
            subject=f"[ALERT] Scheduled job failed: {job_name}",
            plain_text_content=(
                f"Job: {job_name}\n"
                f"Error: {error}\n"
                f"Time: {datetime.now(timezone.utc).isoformat()}\n\n"
                "Check Railway logs for details."
            ),
        )
        _sg.SendGridAPIClient(api_key).send(msg)
    except Exception:
        pass  # Don't let alerting failures mask the original error


def scheduled_cache_warm():
    """Called by APScheduler 3x/day to keep cache fresh."""
    if not _db.try_acquire_job_lock("cache_warm"):
        return  # Another worker is already running this job
    try:
        print(f"[SCHEDULER] Cache warm started at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        cache_clear()
        with app.test_client() as tc:
            tc.get("/api/audit")
            print("[SCHEDULER] Audit cache warmed ✓")
            tc.get("/api/manager")
            print("[SCHEDULER] Manager cache warmed ✓")
            tc.get("/api/isa")
            print("[SCHEDULER] ISA cache warmed ✓")
            tc.get("/api/appointments")
            print("[SCHEDULER] Appointments cache warmed ✓")
        _record_fired("cache_warm")
    except Exception as e:
        _alert_on_job_failure("cache_warm", str(e))
        print(f"[SCHEDULER] Cache warm error: {e}")
        raise
    finally:
        _db.release_job_lock("cache_warm")


def sync_calls_cache():
    """Incremental sync of FUB calls into the calls_cache DB table.

    Uses the newest 'created' timestamp in the cache as the watermark so
    we only fetch records newer than what we already have — keeping each
    batch small and well inside FUB's 2000-record offset cap.

    On first run (empty table) defaults to the last 14 days so the cache
    is seeded with enough history for the 4-week manager scorecard.
    """
    _db.ensure_calls_cache_table()
    watermark = _db.get_calls_cache_watermark()
    if watermark is None:
        since = datetime.now(timezone.utc) - timedelta(days=14)
        print(f"[CALLS CACHE] No watermark — seeding from {since.date()} (14-day backfill)")
    else:
        # Subtract 5 minutes to catch any calls that arrived slightly out-of-order
        since = watermark - timedelta(minutes=5)
        print(f"[CALLS CACHE] Watermark={watermark.isoformat()}, fetching since {since.isoformat()}")

    client = FUBClient()
    calls = client.get_calls(since=since)
    if calls:
        upserted = _db.upsert_calls_cache(calls)
        print(f"[CALLS CACHE] Upserted {upserted} rows ({len(calls)} fetched from FUB)")
    else:
        print("[CALLS CACHE] No new calls from FUB")
    return len(calls) if calls else 0


def scheduled_sync_calls_cache():
    """APScheduler wrapper for sync_calls_cache() — every 30 minutes."""
    if not _db.try_acquire_job_lock("calls_cache_sync"):
        return
    try:
        sync_calls_cache()
    except Exception as e:
        _alert_on_job_failure("calls_cache_sync", str(e))
        print(f"[CALLS CACHE] Sync error: {e}")
    finally:
        _db.release_job_lock("calls_cache_sync")


def scheduled_run_leadstream():
    """Runs every 4 hours — full score (agents + pond) and apply tags."""
    if not _db.try_acquire_job_lock("leadstream"):
        return  # Another worker is already running this job
    try:
        print(f"[SCHEDULER] LeadStream scoring started at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        with app.test_client() as tc:
            resp = tc.post("/api/leadstream/run", json={"apply": True})
            result = resp.get_json() or {}
            print(f"[SCHEDULER] LeadStream: {result.get('status','done')} — "
                  f"{result.get('agent_leads_tagged',0)} agent leads, "
                  f"{result.get('pond_leads_tagged',0)} pond leads tagged")
        _record_fired("leadstream")
    except Exception as e:
        _alert_on_job_failure("leadstream", str(e))
        print(f"[SCHEDULER] LeadStream error: {e}")
        raise
    finally:
        _db.release_job_lock("leadstream")


def scheduled_run_leadstream_pond():
    """Runs hourly — pond-only refresh so contacted leads drop off quickly."""
    print(f"[SCHEDULER] LeadStream pond refresh at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/leadstream/run", json={"apply": True, "pond_only": True})
            result = resp.get_json() or {}
            print(f"[SCHEDULER] Pond refresh: {result.get('pond_leads_tagged',0)} pond leads tagged")
        _record_fired("leadstream_pond")
    except Exception as e:
        print(f"[SCHEDULER] LeadStream pond error: {e}")


def scheduled_leadstream_nightly_cleanup():
    """Runs at 2am ET — deep cleanup of all stale LeadStream tags in FUB."""
    if not _db.try_acquire_job_lock("leadstream_cleanup"):
        return  # Another worker is already running this job
    try:
        print(f"[SCHEDULER] LeadStream nightly cleanup at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        with app.test_client() as tc:
            resp = tc.post("/api/leadstream/deep-cleanup")
            result = resp.get_json() or {}
            print(f"[SCHEDULER] Nightly cleanup: {result.get('removed',0)} tags removed")
        _record_fired("leadstream_cleanup")
    except Exception as e:
        _alert_on_job_failure("leadstream_cleanup", str(e))
        print(f"[SCHEDULER] LeadStream cleanup error: {e}")
        raise
    finally:
        _db.release_job_lock("leadstream_cleanup")


def scheduled_send_audit_email():
    """Monday 8:30am ET — send KPI audit report."""
    if _already_fired_recently("audit_email", within_hours=20):
        print("[SCHEDULER] Audit email: skipped — already sent within 20h")
        return
    print(f"[SCHEDULER] Sending audit email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-email", json={})
            print(f"[SCHEDULER] Audit email: {resp.data.decode()}")
        _record_fired("audit_email")
    except Exception as e:
        print(f"[SCHEDULER] Audit email error: {e}")


def scheduled_send_manager_email():
    """Sunday 3pm ET — send Joe's coaching email."""
    if _already_fired_recently("manager_email", within_hours=20):
        print("[SCHEDULER] Manager email: skipped — already sent within 20h")
        return
    print(f"[SCHEDULER] Sending manager email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-manager-email")
            print(f"[SCHEDULER] Manager email: {resp.data.decode()}")
        _record_fired("manager_email")
    except Exception as e:
        print(f"[SCHEDULER] Manager email error: {e}")


def scheduled_new_lead_check():
    """Every 5 minutes — check Shark Tank for new leads and fire immediate email."""
    try:
        from pond_mailer import run_new_lead_mailer
        result = run_new_lead_mailer(dry_run=False)
        if result.get("sent", 0) > 0:
            print(f"[SCHEDULER] New lead mailer: sent {result['sent']} immediate email(s)")
        _record_fired("new_lead_check")
    except Exception as e:
        print(f"[SCHEDULER] New lead mailer error: {e}")


def scheduled_new_lead_watchdog():
    """
    Every 30 minutes — safety net that ensures no new Shark Tank lead ever
    silently misses outreach.

    The normal new_lead_check fires every 5 min, but Railway redeploys,
    APScheduler misfires, or uncaught exceptions can leave leads stranded.
    This watchdog runs independently, checks the last 24 hours of Shark Tank
    leads, and re-triggers the mailer for anyone who slipped through.

    A single consolidated alert email goes to Barry listing every missed lead
    so he has a paper trail even if the catch-up succeeds.  Alert emails are
    throttled to once per 2 hours so a burst of misses doesn't spam the inbox.
    """
    _WATCHDOG_GRACE_MINUTES   = 45   # min age before we expect outreach to exist
    _WATCHDOG_LOOKBACK_HOURS  = 24   # how far back to scan
    _WATCHDOG_ALERT_COOLDOWN  = "new_lead_watchdog_alert"  # cooldown key
    _WATCHDOG_ALERT_HOURS     = 2    # suppress duplicate alerts within this window

    try:
        api_key = os.environ.get("FUB_API_KEY", "")
        if not api_key:
            print("[WATCHDOG] FUB_API_KEY not set — skipping")
            return

        client   = FUBClient(api_key)
        now_utc  = datetime.now(timezone.utc)
        cutoff   = now_utc - timedelta(hours=_WATCHDOG_LOOKBACK_HOURS)

        # Pull all Shark Tank (pond 4) leads created in the last 24 hours.
        people = client.get_people(pond_id=4, created_since=cutoff, limit=200)
        if not people:
            print("[WATCHDOG] No recent Shark Tank leads — nothing to check")
            return

        missed_email = []   # leads missing email outreach
        missed_sms   = []   # leads missing SMS (informational only)

        for person in people:
            pid       = person.get("id")
            name      = (person.get("name") or "Unknown").strip()
            created   = person.get("created") or ""
            if not pid or not created:
                continue

            # Parse creation time and check age.
            try:
                from dateutil import parser as _dp
                created_dt = _dp.parse(created)
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            age_minutes = (now_utc - created_dt).total_seconds() / 60
            if age_minutes < _WATCHDOG_GRACE_MINUTES:
                continue  # Still within the normal 12-min delay + buffer — too soon

            # Skip leads that are suppressed — they'll never get outreach by design.
            # Counting them as "missed" is a false alarm that fires the alert loop.
            tags = person.get("tags") or []
            from pond_mailer import _email_suppression_tags
            _supp = _email_suppression_tags(tags)
            if _supp:
                print(f"[WATCHDOG] Skipping {name} — suppressed by tag(s): {', '.join(_supp)}")
                continue

            # Check email and SMS outreach logs.
            got_email = _db.has_received_new_lead_immediate(pid)
            got_sms   = _db.has_received_pond_sms(pid)

            if not got_email:
                missed_email.append({
                    "pid":          pid,
                    "name":         name,
                    "age_minutes":  int(age_minutes),
                    "got_sms":      got_sms,
                    "source":       (person.get("source") or "unknown"),
                })
            elif not got_sms:
                # Email fired but SMS didn't — less urgent, just log it.
                missed_sms.append({
                    "pid":         pid,
                    "name":        name,
                    "age_minutes": int(age_minutes),
                    "source":      (person.get("source") or "unknown"),
                })

        if missed_sms and not missed_email:
            # Email fired but SMS didn't — log it. The 5-min new_lead_check will
            # handle the retry automatically; the 7am catch-up covers overnight leads.
            # Do NOT re-trigger the mailer here — that caused race conditions where
            # the watchdog and the scheduler both sent texts to the same lead.
            print(f"[WATCHDOG] {len(missed_sms)} lead(s) have email but no SMS — 5-min checker will retry")

        if not missed_email:
            print(f"[WATCHDOG] All {len(people)} recent Shark Tank lead(s) have email outreach — OK")
            return

        # One or more leads slipped through without email — log and alert only.
        # The 5-min new_lead_check runs independently; re-triggering here causes
        # duplicate sends when both fire within the same window.
        print(f"[WATCHDOG] {len(missed_email)} lead(s) missed email outreach — "
              f"5-min checker will retry on next run")

        # Send Barry one alert email summarising the gap (throttled to once per 2 hrs).
        if _already_fired_recently(_WATCHDOG_ALERT_COOLDOWN, within_hours=_WATCHDOG_ALERT_HOURS):
            print("[WATCHDOG] Alert email suppressed — cooldown active")
            return

        try:
            sg_key = os.environ.get("SENDGRID_API_KEY")
            if not sg_key:
                print("[WATCHDOG] SENDGRID_API_KEY not set — skipping alert email")
                return

            import sendgrid as _sg
            from sendgrid.helpers.mail import Mail as _Mail

            lines = [
                "NEW LEAD OUTREACH WATCHDOG",
                "=" * 52,
                "",
                f"Detected {len(missed_email)} lead(s) that were NOT emailed within",
                f"{_WATCHDOG_GRACE_MINUTES} minutes of registration.",
                "run_new_lead_mailer() was fired automatically to catch up.",
                "",
                "MISSED LEADS:",
            ]
            for m in missed_email:
                sms_note = "no SMS either" if not m["got_sms"] else "SMS sent OK"
                lines.append(
                    f"  {m['name']} (FUB #{m['pid']}) — {m['age_minutes']} min old — "
                    f"{m['source']} — {sms_note}"
                )
            lines += [
                "",
                "WHAT HAPPENED:",
                "  Most likely: Railway redeploy or APScheduler misfire silenced",
                "  new_lead_check during the window these leads came in.",
                "  The catch-up mailer fired above — check Railway logs to confirm",
                "  emails went out.",
                "",
                "WHAT TO DO:",
                "  1. Check your inbox for the outreach audit emails for these leads.",
                "  2. If you don't see them within 5 minutes, check Railway logs.",
                "  3. You can also manually trigger: POST /api/new-lead-check",
                "",
                "=" * 52,
                "Legacy Home Team AI Outreach",
            ]

            html_body = (
                "<div style='font-family:-apple-system,Helvetica,sans-serif;"
                "font-size:14px;line-height:1.7;color:#1a1a1a;"
                "max-width:620px;margin:24px auto;white-space:pre-wrap'>"
                + "\n".join(lines)
                    .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    .replace("\n", "<br>")
                + "</div>"
            )

            plain = "\n".join(lines)
            subject = (
                f"[ALERT] {len(missed_email)} new lead(s) missed outreach — "
                f"catch-up triggered"
            )
            msg = _Mail(
                from_email=config.EMAIL_FROM,
                to_emails="barry@yourfriendlyagent.net",
                subject=subject,
                plain_text_content=plain,
                html_content=html_body,
            )
            _sg.SendGridAPIClient(sg_key).send(msg)
            _record_fired(_WATCHDOG_ALERT_COOLDOWN)
            print(f"[WATCHDOG] Alert email sent — {len(missed_email)} missed lead(s)")

        except Exception as ae:
            print(f"[WATCHDOG] Alert email failed: {ae}")

    except Exception as e:
        print(f"[WATCHDOG] Watchdog error: {e}")


def scheduled_quiet_hours_sms_catchup():
    """
    Daily at 7:02am ET — send SMSes to overnight leads whose send-window block lifted.

    Leads that arrive between 10pm and 7am have their SMS held because PB only sends
    during 7am-10pm ET (iMessage window — not carrier SMS, so TCPA strict hours
    don't apply, but we still respect a reasonable overnight window).
    This job fires 2 minutes after 7am so overnight leads always get their text
    within minutes of the window opening instead of waiting for the next 5-min tick.
    """
    try:
        from pond_mailer import run_new_lead_mailer
        result = run_new_lead_mailer(dry_run=False)
        sms_sent = result.get("sent", 0)
        if sms_sent > 0:
            print(f"[MORNING SMS CATCHUP] Sent SMS to {sms_sent} overnight lead(s)")
        else:
            print(f"[MORNING SMS CATCHUP] No overnight leads waiting for SMS — OK "
                  f"(checked={result.get('checked', 0)}, eligible={result.get('eligible', 0)})")
    except Exception as e:
        print(f"[MORNING SMS CATCHUP] Error: {e}")


def scheduled_serendipity():
    """Every 10 minutes — detect behavioral triggers and fire ready emails."""
    try:
        from serendipity import run_serendipity
        result = run_serendipity(dry_run=False)
        detect  = result.get("detect",  {})
        process = result.get("process", {})
        triggered = detect.get("triggered", 0)
        sent      = process.get("sent", 0)
        if triggered or sent:
            print(f"[SERENDIPITY] {triggered} trigger(s) queued, {sent} email(s) sent")
        _record_fired("serendipity")
    except Exception as e:
        print(f"[SERENDIPITY] Scheduler error: {e}")


def scheduled_run_pond_mailer(daily_cap=45):
    """
    Pond mailer scheduler handler — called by each time-slot job.

    Schedule:
      Mon–Fri  8am / 1pm / 6pm ET   daily_cap=45
      Saturday 10am / 3pm ET        daily_cap=30
      Sunday   1pm  / 6pm ET        daily_cap=30

    daily_cap is enforced inside run_pond_mailer() by checking how many
    emails have already been sent today (ET) and capping this run at the
    remaining allowance.  If the day is already full, the run exits early.
    """
    slot = datetime.now(timezone.utc).strftime("%H:%M UTC")
    print(f"[SCHEDULER] Pond mailer slot firing at {slot} (daily_cap={daily_cap})")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/pond-mailer/run", json={"dry_run": False, "daily_cap": daily_cap})
            result = resp.get_json() or {}
            job_id = result.get("job_id", "unknown")
            print(f"[SCHEDULER] Pond mailer started — job_id: {job_id}")
        _record_fired("pond_mailer")
    except Exception as e:
        print(f"[SCHEDULER] Pond mailer error: {e}")


def scheduled_sync_appointment_tags():
    """Runs 3x/day — sync APT_SET/APT_OUTCOME_NEEDED/APT_STALE tags."""
    if not _db.try_acquire_job_lock("appt_tag_sync"):
        return  # Another worker is already running this job
    try:
        print(f"[SCHEDULER] Syncing appointment tags at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        with app.test_client() as tc:
            resp = tc.post("/api/appointments/sync-tags")
            print(f"[SCHEDULER] Appointment tag sync: {resp.data.decode()}")
        _record_fired("appt_tag_sync")
    except Exception as e:
        _alert_on_job_failure("appt_tag_sync", str(e))
        print(f"[SCHEDULER] Appointment tag sync error: {e}")
        raise
    finally:
        _db.release_job_lock("appt_tag_sync")


def _isa_sync_stages(client, active_transfers):
    """Move any ISA transfer lead still in 'Lead' stage → 'Warm'.

    Called by the daily ISA transfer job. Runs before expiry so newly-tagged
    leads get the stage bump on their first day, not just at the 7-day mark.
    """
    from config import ISA_TRANSFER_WARM_STAGE
    moved = 0
    for row in active_transfers:
        person_id = row["person_id"]
        try:
            person = client.get_person(person_id)
            if (person.get("stage") or "").strip().lower() == "lead":
                client._request("PUT", f"people/{person_id}",
                                json_data={"stage": ISA_TRANSFER_WARM_STAGE})
                moved += 1
                print(f"[ISA] Stage: {row.get('lead_name', person_id)} Lead → {ISA_TRANSFER_WARM_STAGE}")
        except Exception as e:
            print(f"[ISA] Stage sync failed for {person_id}: {e}")
    return moved


def _isa_create_escalation_task(client, row):
    """Create a FUB task on the agent for an unworked ISA transfer at day 7.

    Only fires if the lead is still in Lead/Warm stage — if the agent advanced
    it to Hot or beyond, we assume they connected and skip the escalation.
    Task is assigned to the agent so it appears in their FUB task list.
    """
    person_id  = row["person_id"]
    lead_name  = row.get("lead_name") or "this lead"
    agent_name = row.get("agent_name") or "the agent"
    try:
        person           = client.get_person(person_id)
        stage            = (person.get("stage") or "").strip().lower()
        assigned_user_id = person.get("assignedUserId")
        if not assigned_user_id:
            return False
        # Only escalate if still in early stages
        if stage in ("lead", "warm", ""):
            client.create_task(
                person_id=person_id,
                assigned_user_id=assigned_user_id,
                name=(f"⚠️ ISA Transfer — 7 days, no connection: {lead_name}"),
                description=(
                    f"{lead_name} was handed to you by ISA 7 days ago after "
                    f"Ylopo's AI confirmed they were ready. No connected call "
                    f"recorded yet. Reach out today or flag for Barry."
                ),
            )
            print(f"[ISA] Escalation task created for {lead_name} ({agent_name})")
            return True
    except Exception as e:
        print(f"[ISA] Escalation task failed for {person_id}: {e}")
    return False


def scheduled_expire_isa_transfers():
    """Daily 6:05am ET — three-step ISA transfer maintenance:

    1. Stage sync: move any fresh-transfer lead still in 'Lead' → 'Warm'
       (runs on ALL active transfers, so newly-tagged leads get bumped today)
    2. Expiry: remove ISA_TRANSFER_FRESH tag from leads 7+ days old so
       LeadStream resumes normal aging
    3. Day-8 escalation: for each expired lead still in Lead/Warm stage
       (agent never connected), create a FUB task as a coaching trigger
    """
    if not _db.try_acquire_job_lock("isa_transfer_expire"):
        return
    try:
        from config import ISA_TRANSFER_FRESH_TAG, ISA_TRANSFER_FRESH_DAYS
        api_key = os.environ.get("FUB_API_KEY", "")
        from fub_client import FUBClient
        client = FUBClient(api_key)

        # ── Step 1: Stage sync (all active transfers) ──────────────────────
        active = _db.get_all_isa_transfers()
        stage_moved = _isa_sync_stages(client, active) if active else 0
        print(f"[SCHEDULER] ISA stage sync: {stage_moved} leads moved to Warm")

        # ── Step 2: Expire old tags ─────────────────────────────────────────
        expired = _db.get_expired_isa_transfers(days=ISA_TRANSFER_FRESH_DAYS)
        if not expired:
            print("[SCHEDULER] ISA transfer expire: no expired transfers")
            _record_fired("isa_transfer_expire")
            return

        print(f"[SCHEDULER] ISA transfer expire: {len(expired)} leads at day 7+")
        removed, failed = 0, 0
        for row in expired:
            person_id = row["person_id"]
            try:
                client.remove_tag(person_id, ISA_TRANSFER_FRESH_TAG)
                _db.delete_isa_transfer(person_id)
                removed += 1
            except Exception as e:
                failed += 1
                print(f"[SCHEDULER] ISA expire failed for {person_id}: {e}")

        # ── Step 3: Day-8 escalation — create FUB task if still unworked ──
        escalated = 0
        for row in expired:
            if _isa_create_escalation_task(client, row):
                escalated += 1

        print(f"[SCHEDULER] ISA transfer expire: {removed} removed, "
              f"{failed} failed, {escalated} escalation tasks created")
        _record_fired("isa_transfer_expire")
    except Exception as e:
        _alert_on_job_failure("isa_transfer_expire", str(e))
        print(f"[SCHEDULER] ISA transfer expire error: {e}")
        raise
    finally:
        _db.release_job_lock("isa_transfer_expire")


def scheduled_send_appointment_email():
    """Tuesday 9am ET — send appointment accountability email."""
    if _already_fired_recently("appt_email", within_hours=20):
        print("[SCHEDULER] Appointment email: skipped — already sent within 20h")
        return
    print(f"[SCHEDULER] Sending appointment email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-appointment-email")
            print(f"[SCHEDULER] Appointment email: {resp.data.decode()}")
        _record_fired("appt_email")
    except Exception as e:
        print(f"[SCHEDULER] Appointment email error: {e}")


def scheduled_send_isa_email():
    """Monday 10am ET — send Fhalen's ISA email."""
    if _already_fired_recently("isa_email", within_hours=20):
        print("[SCHEDULER] ISA email: skipped — already sent within 20h")
        return
    print(f"[SCHEDULER] Sending ISA email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-isa-email")
            print(f"[SCHEDULER] ISA email: {resp.data.decode()}")
        _record_fired("isa_email")
    except Exception as e:
        print(f"[SCHEDULER] ISA email error: {e}")


def _recalc_all_streaks():
    """Nightly: recalculate and persist streaks for all agents."""
    profiles = _db.get_agent_profiles(active_only=True)
    identities = _db.get_all_agent_identities()
    for p in profiles:
        name = p["agent_name"]
        ident = identities.get(name, {})
        targets = {"daily_calls_target": ident.get("daily_calls_target", 1)}
        _db.update_streak(name, targets)


def sync_fub_roster():
    """
    Pull all active agents from FUB and upsert into agent_profiles.
    - Updates fub_phone (never overwrites user-provided phone)
    - Detects NEW agents and sends them the goal setup onboarding email
    - Ensures every agent has a goal token (creates one if missing)
    Runs every 4 hours via scheduler so new hires get onboarded same day.
    """
    if not _db.is_available():
        print("[ROSTER SYNC] DB not available — skipping")
        return {"synced": 0, "new": 0, "errors": 0}

    try:
        fub = FUBClient()
        fub_agents = fub.get_agents_with_email(excluded_names=config.EXCLUDED_USERS)
    except Exception as e:
        print(f"[ROSTER SYNC] FUB API error: {e}")
        return {"synced": 0, "new": 0, "errors": 1}

    synced = 0
    new_agents = []
    errors = 0

    for agent in fub_agents:
        name      = agent["name"]
        fub_uid   = agent["fub_user_id"]
        email     = agent["email"]
        fub_phone = agent.get("fub_phone")
        try:
            is_new = _db.upsert_agent_from_fub_roster(
                agent_name=name,
                fub_user_id=fub_uid,
                email=email,
                fub_phone=fub_phone,
                is_active=True
            )
            # Every agent needs a goal token — create if missing
            if not _db.get_token_for_agent(name):
                _db.create_goal_token(name)
            synced += 1
            if is_new:
                new_agents.append({"name": name, "email": email})
        except Exception as e:
            print(f"[ROSTER SYNC] Error upserting {name}: {e}")
            errors += 1

    # Mark agents no longer in FUB as inactive
    fub_names = {a["name"] for a in fub_agents}
    all_active = _db.get_agent_profiles(active_only=True)
    for profile in all_active:
        if profile["agent_name"] not in fub_names and profile["agent_name"] not in config.EXCLUDED_USERS:
            _db.upsert_agent_profile(profile["agent_name"], is_active=False)
            print(f"[ROSTER SYNC] Marked {profile['agent_name']} as inactive (not in FUB)")

    # Send onboarding emails to newly detected agents
    for agent in new_agents:
        try:
            name  = agent["name"]
            first = name.split()[0]
            token = _db.get_token_for_agent(name)
            base_url      = os.environ.get("BASE_URL", "").rstrip("/")
            setup_url     = f"{base_url}/goals/setup/{token}" if base_url and token else ""
            dashboard_url = f"{base_url}/my-goals/{token}" if base_url and token else ""
            if setup_url and agent["email"]:
                from email_report import send_goal_onboarding_email
                send_goal_onboarding_email(name, first, agent["email"], setup_url, dashboard_url=dashboard_url)
                _db.mark_onboarding_sent(name)
                print(f"[ROSTER SYNC] Onboarding email sent → {name} <{agent['email']}>")
            else:
                print(f"[ROSTER SYNC] New agent {name} — no setup URL or email, skipping email")
        except Exception as e:
            print(f"[ROSTER SYNC] Onboarding email failed for {agent['name']}: {e}")

    print(f"[ROSTER SYNC] Done: {synced} synced, {len(new_agents)} new, {errors} errors")
    return {"synced": synced, "new": len(new_agents), "errors": errors}


def scheduled_sync_fub_roster():
    """Scheduler wrapper for sync_fub_roster()."""
    if not _db.try_acquire_job_lock("roster_sync"):
        return  # Another worker is already running this job
    try:
        sync_fub_roster()
    except Exception as e:
        _alert_on_job_failure("roster_sync", str(e))
        print(f"[ROSTER SYNC] Unhandled error in scheduled run: {e}")
        raise
    finally:
        _db.release_job_lock("roster_sync")


def sync_daily_activity_from_fub(target_date=None):
    """
    Pull call and appointment counts from FUB for a single date and upsert
    into daily_activity using GREATEST (manual entries that are higher are
    never overwritten).

    target_date: a datetime.date object, defaults to yesterday ET.
    """
    if not _db.is_available():
        print("[ACTIVITY SYNC] DB not available — skipping")
        return

    _et_h = -4 if 3 <= datetime.now(timezone.utc).month <= 10 else -5
    ET = timezone(timedelta(hours=_et_h))

    if target_date is None:
        now_et      = datetime.now(ET)
        target_date = (now_et - timedelta(days=1)).date()

    print(f"[ACTIVITY SYNC] Syncing FUB activity for {target_date}…")

    try:
        fub = FUBClient()

        d_start_utc = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=ET).astimezone(timezone.utc)
        d_end_utc   = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=ET).astimezone(timezone.utc)

        # ── Calls ──────────────────────────────────────────────────────
        # Count outbound calls only (isIncoming==False) — matches FUB's "Calls Made" metric.
        # Also count conversations separately (any direction, duration >= threshold).
        # This mirrors count_calls_for_user() in kpi_audit.py so the nudge engine
        # and the KPI dashboard always agree on the same numbers.
        all_calls = fub.get_calls(since=d_start_utc, until=d_end_utc)
        calls_by_uid  = {}   # outbound dials per user
        convos_by_uid = {}   # conversations (≥ threshold) per user
        _convo_thresh = config.CONVERSATION_THRESHOLD_SECONDS
        _excl_uids    = config.EXCLUDED_CALL_USER_IDS
        for c in all_calls:
            uid = c.get("userId")
            if not uid or uid in _excl_uids:
                continue
            is_incoming = c.get("isIncoming", False)
            duration    = c.get("duration", 0) or 0
            # Outbound dials
            if not is_incoming:
                calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1
            # Conversations: any call long enough to be real
            if duration >= _convo_thresh:
                convos_by_uid[uid] = convos_by_uid.get(uid, 0) + 1

        # ── Appointments ───────────────────────────────────────────────
        all_appts = fub.get_appointments(since=d_start_utc, until=d_end_utc)
        # FUB does NOT reliably expose userId at the appointment root.
        # Must use invitees array, same as count_appointments_for_user.
        appts_by_uid = {}
        for a in all_appts:
            invitees = a.get("invitees") or []
            has_lead = any(inv.get("personId") for inv in invitees)
            if not has_lead:
                continue
            for inv in invitees:
                uid = inv.get("userId")
                if uid:
                    appts_by_uid[uid] = appts_by_uid.get(uid, 0) + 1

        # ── Upsert per agent ───────────────────────────────────────────
        agents  = _db.get_agent_profiles(active_only=True)
        updated = 0
        for agent in agents:
            fub_uid = agent.get("fub_user_id")
            if not fub_uid:
                continue
            calls  = calls_by_uid.get(fub_uid, 0)
            convos = convos_by_uid.get(fub_uid, 0)
            appts  = appts_by_uid.get(fub_uid, 0)
            if calls > 0 or convos > 0 or appts > 0:
                _db.upsert_daily_activity_fub(agent["agent_name"], target_date, calls, appts, convos)
                updated += 1

        print(f"[ACTIVITY SYNC] Done — {updated} agents updated for {target_date} "
              f"(calls pool: {len(calls_by_uid)}, appts pool: {len(appts_by_uid)})")
    except Exception as e:
        print(f"[ACTIVITY SYNC] Error for {target_date}: {e}")


def sync_week_activity_from_fub():
    """
    Backfill Mon–yesterday for the current work week from FUB.
    Called before Sunday morning nudges so weekly totals are accurate
    even if any nightly syncs were missed (e.g. after a crash/redeploy).
    """
    from datetime import date as _date
    today      = _date.today()
    # today.weekday(): Mon=0 … Sun=6 → days back to Monday
    days_back  = today.weekday()   # 6 on Sunday → Mon is 6 days ago
    week_start = today - timedelta(days=days_back)
    week_end   = today - timedelta(days=1)       # yesterday (Sat)

    print(f"[ACTIVITY SYNC] Week backfill {week_start} → {week_end}")
    d = week_start
    while d <= week_end:
        sync_daily_activity_from_fub(target_date=d)
        d += timedelta(days=1)


def scheduled_sync_daily_activity():
    """Scheduler wrapper for sync_daily_activity_from_fub().

    Syncs both yesterday (complete day) and today (partial — so agents
    see their current-day appointments and calls on the dashboard).
    """
    if not _db.try_acquire_job_lock("activity_sync"):
        return  # Another worker is already running this job
    try:
        _et_h = -4 if 3 <= datetime.now(timezone.utc).month <= 10 else -5
        ET = timezone(timedelta(hours=_et_h))
        today_et = datetime.now(ET).date()
        yesterday_et = today_et - timedelta(days=1)

        sync_daily_activity_from_fub(target_date=yesterday_et)  # complete day
        sync_daily_activity_from_fub(target_date=today_et)      # partial — current day
    except Exception as e:
        _alert_on_job_failure("activity_sync", str(e))
        print(f"[ACTIVITY SYNC] Unhandled error in scheduled run: {e}")
        raise
    finally:
        _db.release_job_lock("activity_sync")


def scheduled_sync_goals_data():
    """
    Scheduled daily job (5:45am ET):
    1. Sync FUB Deals → deal_log (contracts + closings from Dotloop)
    2. Compute YTD calls + appointments per agent → agent_ytd_cache

    This keeps the manager scorecard fast (reads from DB, no live FUB calls)
    and means the data is always fresh without anyone clicking a button.
    """
    if not _db.try_acquire_job_lock("goals_data_sync"):
        return  # Another worker is already running this job
    if not _db.is_available():
        print("[GOALS SYNC] DB not available — skipping")
        _db.release_job_lock("goals_data_sync")
        return

    year = datetime.now(timezone.utc).year
    print(f"[GOALS SYNC] Starting goals data sync for {year}…")

    try:
        # ── 0. Sync FUB agent roster → agent_profiles ──────────────────────
        # Runs first so new agents added since last sync appear on the scorecard
        # before we try to pull their deals / call counts.
        try:
            _rc = FUBClient()
            _excluded = list(config.EXCLUDED_USERS)
            _roster = _rc.get_agents_with_email(excluded_names=_excluded)
            for _a in _roster:
                _db.upsert_agent_profile(
                    _a["name"],
                    fub_user_id=_a["fub_user_id"],
                    email=_a["email"],
                    is_active=True,
                )
            print(f"[GOALS SYNC] Roster sync: {len(_roster)} agents upserted")
        except Exception as _re:
            print(f"[GOALS SYNC] Roster sync failed (non-fatal): {_re}")

        # ── 1. Sync FUB Deals ──────────────────────────────────────────────
        try:
            client = FUBClient()
            since = datetime(year, 1, 1, tzinfo=timezone.utc)
            deals = client.get_deals(since=since)

            all_goals = _db.get_all_goals(year=year)
            comm_lookup = {g["agent_name"]: float(g["commission_pct"]) for g in all_goals}

            # Build FUB user_id → agent_name map for accurate matching
            profiles = _db.get_agent_profiles(active_only=False)
            uid_to_name = {p["fub_user_id"]: p["agent_name"] for p in profiles if p["fub_user_id"]}

            def _pd(val):
                """Parse an ISO date string or None — defined once, used per deal."""
                if not val:
                    return None
                try:
                    from datetime import date as _d
                    return _d.fromisoformat(str(val)[:10])
                except Exception:
                    return None

            synced = skipped = 0
            for deal in deals:
                fub_deal_id = deal.get("id")
                if not fub_deal_id:
                    continue

                stage_raw = (deal.get("stage") or deal.get("stageName") or
                             deal.get("stageLabel") or "")

                # Prefer user-ID match; fall back to name string
                agent_uid = deal.get("assignedUserId") or deal.get("userId")
                agent_name = uid_to_name.get(agent_uid)
                if not agent_name:
                    raw = deal.get("assignedTo") or ""
                    agent_name = raw if isinstance(raw, str) else (
                        f"{raw.get('firstName','')} {raw.get('lastName','')}".strip()
                        if isinstance(raw, dict) else ""
                    )

                sale_price = deal.get("price") or deal.get("salePrice") or 0
                deal_name  = deal.get("name") or deal.get("address") or f"Deal #{fub_deal_id}"

                close_date    = _pd(deal.get("closedAt") or deal.get("closeDate"))
                contract_date = _pd(deal.get("contractDate") or deal.get("created"))
                comm_pct      = comm_lookup.get(agent_name)

                result = _db.upsert_deal(fub_deal_id, agent_name, deal_name, sale_price,
                                         stage_raw, contract_date, close_date,
                                         commission_pct=comm_pct)
                if result:
                    synced += 1
                else:
                    skipped += 1

            print(f"[GOALS SYNC] Deals: {synced} synced, {skipped} skipped/unrecognised")
        except Exception as e:
            print(f"[GOALS SYNC] Deal sync error: {e}")

        # ── 2. Cache YTD calls + appointments per agent ────────────────────
        try:
            year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
            ytd_calls = client.get_calls(since=year_start)
            ytd_appts = client.get_appointments(since=year_start)

            fub_users = {
                f"{u.get('firstName','')} {u.get('lastName','')}".strip(): u.get("id")
                for u in client.get_users()
            }

            _convo_thresh = config.CONVERSATION_THRESHOLD_SECONDS
            calls_by_uid  = {}
            convos_by_uid = {}
            for call in ytd_calls:
                if call.get("isIncoming"):
                    continue
                uid = call.get("userId")
                if not uid:
                    continue
                calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1
                dur = call.get("duration", 0) or 0
                if dur >= _convo_thresh:
                    convos_by_uid[uid] = convos_by_uid.get(uid, 0) + 1

            # FUB does NOT reliably expose userId at the appointment root.
            # Must check invitees array — same logic as count_appointments_for_user
            # in kpi_audit.py. Only count if a lead (personId) is attached.
            appts_by_uid = {}
            for appt in ytd_appts:
                invitees = appt.get("invitees") or []
                has_lead = any(inv.get("personId") for inv in invitees)
                if not has_lead:
                    continue
                for inv in invitees:
                    uid = inv.get("userId")
                    if uid:
                        appts_by_uid[uid] = appts_by_uid.get(uid, 0) + 1

            cached = 0
            for agent_name, fub_uid in fub_users.items():
                if agent_name in config.EXCLUDED_USERS:
                    continue
                calls = calls_by_uid.get(fub_uid, 0)
                convos = convos_by_uid.get(fub_uid, 0)
                appts = appts_by_uid.get(fub_uid, 0)
                if _db.upsert_ytd_cache(agent_name, year, calls, appts, convos_ytd=convos):
                    cached += 1

            print(f"[GOALS SYNC] YTD cache updated for {cached} agents")
        except Exception as e:
            print(f"[GOALS SYNC] YTD cache error: {e}")
            _alert_on_job_failure("goals_data_sync", str(e))

        print("[GOALS SYNC] Done.")
    finally:
        _db.release_job_lock("goals_data_sync")


def scheduled_onboarding_escalation():
    """
    Daily job (8am ET):
    1. Send initial onboarding email to any active agent whose onboarding_sent_at is NULL
       (catches pre-existing agents and anyone missed by roster sync).
    2. Send Day 3 reminder to agents who got the email 3 days ago but haven't set goals.
    3. Send Day 7 reminder to agents who got the email 7 days ago but haven't set goals.
    """
    if not _db.try_acquire_job_lock("onboarding_escalation"):
        return
    try:
        base_url = os.environ.get("BASE_URL", "").rstrip("/")

        # ── Step 1: send initial email to anyone who never got it ──────────
        unsent = _db.get_agents_needing_onboarding()
        for agent in unsent:
            name  = agent["agent_name"]
            first = name.split()[0]
            email = agent.get("email")
            token = _db.get_token_for_agent(name)
            setup_url     = f"{base_url}/goals/setup/{token}" if base_url and token else ""
            dashboard_url = f"{base_url}/my-goals/{token}"   if base_url and token else ""
            if not email or not setup_url:
                continue
            try:
                from email_report import send_goal_onboarding_email
                send_goal_onboarding_email(name, first, email, setup_url,
                                           dashboard_url=dashboard_url)
                _db.mark_onboarding_sent(name)
                print(f"[ONBOARDING] Initial email sent to {name}")
            except Exception as e:
                print(f"[ONBOARDING] Initial email failed for {name}: {e}")

        # ── Step 2 & 3: Day 3 / Day 7 follow-ups ──────────────────────────
        agents_needed = _db.get_agents_no_goal_setup()
        today = datetime.now(timezone.utc).date()

        for agent in agents_needed:
            sent_at = agent.get("onboarding_sent_at")
            if not sent_at:
                continue
            try:
                sent_date = datetime.fromisoformat(sent_at.replace("Z", "+00:00")).date()
            except Exception:
                continue
            days_since = (today - sent_date).days
            name  = agent["agent_name"]
            first = name.split()[0]
            email = agent.get("email")
            token = _db.get_token_for_agent(name)
            base_url  = os.environ.get("BASE_URL", "").rstrip("/")
            setup_url = f"{base_url}/goals/setup/{token}" if base_url and token else ""

            if days_since == 3 and email and setup_url:
                # Day 3: reminder email
                try:
                    from email_report import send_goal_onboarding_reminder
                    send_goal_onboarding_reminder(name, first, email, setup_url, day=3)
                    print(f"[ONBOARDING] Day 3 reminder sent to {name}")
                except Exception as e:
                    print(f"[ONBOARDING] Day 3 email failed for {name}: {e}")

            elif days_since == 7:
                # Day 7: email + text if phone available
                if email and setup_url:
                    try:
                        from email_report import send_goal_onboarding_reminder
                        send_goal_onboarding_reminder(name, first, email, setup_url, day=7)
                        print(f"[ONBOARDING] Day 7 reminder sent to {name}")
                    except Exception as e:
                        print(f"[ONBOARDING] Day 7 email failed for {name}: {e}")
                # Day 7 follow-up email already handles this — no SMS fallback needed
    except Exception as e:
        _alert_on_job_failure("onboarding_escalation", str(e))
        raise
    finally:
        _db.release_job_lock("onboarding_escalation")


def scheduled_gone_dark_alert():
    """
    Weekly job (Monday 7am ET): email Barry a list of agents who haven't
    logged any activity in 10+ days so he can reach out directly.
    """
    if not _db.try_acquire_job_lock("gone_dark_alert"):
        return
    try:
        gone_dark = _db.get_agents_gone_dark(days=10)
        if not gone_dark:
            print("[GONE DARK] No agents gone dark — skipping email")
            return

        api_key = os.environ.get("SENDGRID_API_KEY")
        if not api_key:
            return
        import sendgrid as _sg
        from sendgrid.helpers.mail import Mail as _Mail

        rows = "\n".join(
            f"  - {a['agent_name']} (last active: {a['last_activity'] or 'never'})"
            for a in gone_dark
        )
        msg = _Mail(
            from_email=config.EMAIL_FROM,
            to_emails=config.EMAIL_FROM,
            subject=f"⚠ {len(gone_dark)} Agent(s) Gone Dark — No Activity in 10+ Days",
            plain_text_content=(
                f"These agents haven't logged any activity in 10+ days:\n\n{rows}\n\n"
                "Reach out directly. They may have disengaged from the system.\n\n"
                "— Legacy Home Team Automated Alert"
            ),
        )
        _sg.SendGridAPIClient(api_key).send(msg)
        print(f"[GONE DARK] Alert sent — {len(gone_dark)} agents")
    except Exception as e:
        _alert_on_job_failure("gone_dark_alert", str(e))
    finally:
        _db.release_job_lock("gone_dark_alert")


def _run_morning_jobs():
    """Run morning nudges then closing milestones independently so a crash in
    the first job does not prevent the second from running."""
    import nudge_engine as _nudge_local   # explicit local import — avoids NameError
    from datetime import date as _date

    # Sunday: backfill Mon–Sat from FUB before building leaderboard
    if _date.today().weekday() == 6:
        try:
            sync_week_activity_from_fub()
        except Exception as e:
            logger.error("sync_week_activity_from_fub crashed: %s", e)

    try:
        _nudge_local.run_morning_nudges()
    except Exception as e:
        logger.error("run_morning_nudges crashed: %s", e)
    try:
        _nudge_local.run_closing_milestones()
    except Exception as e:
        logger.error("run_closing_milestones crashed: %s", e)


def start_scheduler():
    """Start APScheduler with all scheduled jobs."""
    global _scheduler_started, _scheduler
    if _scheduler_started:
        return
    _scheduler_started = True

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("[SCHEDULER] APScheduler not installed — skipping scheduled jobs")
        return

    _scheduler = BackgroundScheduler(timezone="US/Eastern")
    ET = "US/Eastern"  # passed explicitly to every CronTrigger so Railway (UTC) honors ET

    # New lead immediate mailer: every 5 minutes
    # Checks Shark Tank (pond 4) for leads created in the last 45 min,
    # fires "caught at the computer" email after the 12-minute delay buffer.
    _scheduler.add_job(scheduled_new_lead_check, CronTrigger(minute="*/5", timezone=ET),
                       id="new_lead_check", name="New lead immediate mailer (every 5 min)",
                       max_instances=1, misfire_grace_time=60)

    # New lead watchdog: every 30 minutes
    # Independent safety net — checks last 24hrs of Shark Tank leads for anyone
    # who never got an email, re-triggers the mailer, and alerts Barry.
    # Runs offset from new_lead_check (at :15 and :45) so it doesn't pile on.
    _scheduler.add_job(scheduled_new_lead_watchdog, CronTrigger(minute="15,45", timezone=ET),
                       id="new_lead_watchdog", name="New lead outreach watchdog (every 30 min)",
                       max_instances=1, misfire_grace_time=120)

    # Morning quiet-hours SMS catch-up: daily at 7:02am ET
    # Fires run_new_lead_mailer() exactly 2 minutes after the iMessage send window
    # opens (7am). PB sends via iMessage, not carrier SMS, so the window is 7am-10pm
    # instead of 8am-9pm. Catches overnight leads whose SMS was held.
    _scheduler.add_job(scheduled_quiet_hours_sms_catchup,
                       CronTrigger(hour=7, minute=2, timezone=ET),
                       id="morning_sms_catchup", name="Morning quiet-hours SMS catch-up (7:02am ET)",
                       max_instances=1, misfire_grace_time=300)

    # Serendipity Clause: every 10 minutes
    # Scans FUB events for behavioral triggers (save, repeat view, inactivity return)
    # and fires queued emails once their human-feeling delay window elapses.
    _scheduler.add_job(scheduled_serendipity, CronTrigger(minute="*/10", timezone=ET),
                       id="serendipity", name="Serendipity Clause (every 10 min)",
                       max_instances=1, misfire_grace_time=120)

    # Cache warming: 3x/day at 6am, 12pm, 6pm ET
    _scheduler.add_job(scheduled_cache_warm, CronTrigger(hour="6,12,18", minute=0, timezone=ET),
                       id="cache_warm", name="Cache warm (3x/day)")

    # LeadStream full scoring: 4x/day at 8am, 12pm, 4pm, 8pm ET
    _scheduler.add_job(scheduled_run_leadstream, CronTrigger(hour="8,12,16,20", minute=7, timezone=ET),
                       id="leadstream", name="LeadStream scoring (4x/day)",
                       max_instances=1, misfire_grace_time=300)

    # LeadStream pond refresh: hourly at :37
    _scheduler.add_job(scheduled_run_leadstream_pond, CronTrigger(minute=37, timezone=ET),
                       id="leadstream_pond", name="LeadStream pond refresh (hourly)",
                       max_instances=1, misfire_grace_time=120)

    # LeadStream nightly cleanup: 3am ET
    _scheduler.add_job(scheduled_leadstream_nightly_cleanup, CronTrigger(hour=3, minute=0, timezone=ET),
                       id="leadstream_cleanup", name="LeadStream nightly cleanup (3am)",
                       max_instances=1, misfire_grace_time=600)

    # Appointment tag sync: 3x/day at 7am, 1pm, 7pm ET
    _scheduler.add_job(scheduled_sync_appointment_tags, CronTrigger(hour="7,13,19", minute=0, timezone=ET),
                       id="appt_tag_sync", name="Appointment tag sync (3x/day)")

    # ISA transfer fresh tag expiry: daily 6:05am ET
    # Removes ISA_TRANSFER_FRESH from leads where Barry's FUB automation added it
    # 7+ days ago, so LeadStream resumes normal scoring after the priority window.
    _scheduler.add_job(scheduled_expire_isa_transfers, CronTrigger(hour=6, minute=5, timezone=ET),
                       id="isa_transfer_expire", name="ISA transfer fresh tag expiry (daily 6:05am)",
                       max_instances=1, misfire_grace_time=600)

    # Joe's coaching email: Sunday 3pm ET
    # No misfire_grace_time: if server is down at send time, skip it — never retry.
    # Retrying after restart is what caused duplicate emails.
    _scheduler.add_job(scheduled_send_manager_email, CronTrigger(day_of_week="sun", hour=15, minute=0, timezone=ET),
                       id="manager_email", name="Joe's Sunday coaching email",
                       max_instances=1, coalesce=True)

    # KPI Audit email: Monday 8:30am ET
    _scheduler.add_job(scheduled_send_audit_email, CronTrigger(day_of_week="mon", hour=8, minute=30, timezone=ET),
                       id="audit_email", name="Monday KPI audit email",
                       max_instances=1, coalesce=True)

    # ── Pond Mailer schedule ────────────────────────────────────────────────
    # Mon–Fri: 3× at 8am, 1pm, 6pm ET — 45 emails/day cap
    # Saturday: 2× at 10am, 3pm ET    — 30 emails/day cap
    # Sunday:   2× at 1pm, 6pm ET     — 30 emails/day cap
    # daily_cap enforced inside run_pond_mailer() via DB count of today's sends.
    # max_instances=1 prevents overlapping runs; coalesce=True fires once on
    # restart if a slot was missed (e.g., Railway redeploy hit at send time).
    _pond_cap_wkday = {"daily_cap": 45}
    _pond_cap_wkend = {"daily_cap": 30}
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="mon-fri", hour=8,  minute=0,  timezone=ET),
                       id="pond_mailer_wkd_8am",  name="Pond mailer Mon-Fri 8am",
                       kwargs=_pond_cap_wkday, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="mon-fri", hour=13, minute=0,  timezone=ET),
                       id="pond_mailer_wkd_1pm",  name="Pond mailer Mon-Fri 1pm",
                       kwargs=_pond_cap_wkday, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="mon-fri", hour=18, minute=0,  timezone=ET),
                       id="pond_mailer_wkd_6pm",  name="Pond mailer Mon-Fri 6pm",
                       kwargs=_pond_cap_wkday, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="sat",     hour=10, minute=0,  timezone=ET),
                       id="pond_mailer_sat_10am", name="Pond mailer Sat 10am",
                       kwargs=_pond_cap_wkend, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="sat",     hour=15, minute=0,  timezone=ET),
                       id="pond_mailer_sat_3pm",  name="Pond mailer Sat 3pm",
                       kwargs=_pond_cap_wkend, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="sun",     hour=13, minute=0,  timezone=ET),
                       id="pond_mailer_sun_1pm",  name="Pond mailer Sun 1pm",
                       kwargs=_pond_cap_wkend, max_instances=1, coalesce=True)
    _scheduler.add_job(scheduled_run_pond_mailer, CronTrigger(day_of_week="sun",     hour=18, minute=0,  timezone=ET),
                       id="pond_mailer_sun_6pm",  name="Pond mailer Sun 6pm",
                       kwargs=_pond_cap_wkend, max_instances=1, coalesce=True)

    # ── Nudge engine jobs ──────────────────────────────────────────────────
    try:
        import nudge_engine as _nudge

        # Morning nudges + closing milestones: once daily at 8am ET
        _scheduler.add_job(
            _run_morning_jobs,
            CronTrigger(hour=8, minute=0, timezone=ET),
            id="nudge_morning", name="Morning nudges + closing milestones (8am ET daily)",
            max_instances=1, coalesce=True,
        )
        # 5pm afternoon push removed — Arc engine 8am email handles all scenarios.
        # Generic templates don't represent the system we built.
        # Streak break + recalculate: 7:15am ET daily (before morning nudges)
        _scheduler.add_job(
            lambda: (_nudge.run_streak_break_check(), _recalc_all_streaks()),
            CronTrigger(hour=7, minute=15, timezone=ET),
            id="nudge_streak_break", name="Streak break check (7:15am ET)",
            max_instances=1, coalesce=True,
        )
        # Weekly summary: Sunday 6pm ET
        _scheduler.add_job(
            lambda: _nudge.run_weekly_summary(),
            CronTrigger(day_of_week="sun", hour=18, minute=0, timezone=ET),
            id="nudge_weekly", name="Weekly summary email (Sunday 6pm)",
            max_instances=1, coalesce=True,
        )
        # Plateau check: every Monday morning
        _scheduler.add_job(
            lambda: _nudge.run_plateau_check(),
            CronTrigger(day_of_week="mon", hour=7, minute=30, timezone=ET),
            id="nudge_plateau", name="Plateau detection (Monday 7:30am)",
            max_instances=1, coalesce=True,
        )
        # Post-closing follow-ups: daily 9am ET
        _scheduler.add_job(
            lambda: _nudge.run_post_closing_followups(),
            CronTrigger(hour=9, minute=0, timezone=ET),
            id="nudge_post_closing", name="Post-closing follow-ups (9am daily)",
            max_instances=1, coalesce=True,
        )
        logger.info("[SCHEDULER] Nudge engine jobs added")
    except Exception as _e:
        logger.warning("[SCHEDULER] Nudge engine failed to load (Twilio not required to run): %s", _e)

    # Fhalen ISA email: Monday 10am ET
    _scheduler.add_job(scheduled_send_isa_email, CronTrigger(day_of_week="mon", hour=10, minute=0, timezone=ET),
                       id="isa_email", name="Monday ISA email",
                       max_instances=1, coalesce=True)

    # Appointment accountability email: Tuesday 9am ET
    _scheduler.add_job(scheduled_send_appointment_email, CronTrigger(day_of_week="tue", hour=9, minute=0, timezone=ET),
                       id="appt_email", name="Tuesday appointment email",
                       max_instances=1, coalesce=True)

    # Bounce sync: daily 5:50am ET
    # Fetches all hard bounces from SendGrid and tags matching FUB leads with BAD_EMAIL.
    # Runs before the morning mailer so dead addresses are already flagged.
    _scheduler.add_job(scheduled_sync_bounces,
                       CronTrigger(hour=5, minute=50, timezone=ET),
                       id="bounce_sync", name="SendGrid bounce → FUB BAD_EMAIL tag (daily 5:50am)",
                       max_instances=1, coalesce=True)

    # Goals data sync: daily 5:45am ET
    # Syncs FUB Deals (from Dotloop) and caches YTD call/appt counts per agent.
    # Keeps the manager scorecard fast and current without manual clicking.
    _scheduler.add_job(scheduled_sync_goals_data,
                       CronTrigger(hour=5, minute=45, timezone=ET),
                       id="goals_sync", name="Goals data sync (daily 5:45am ET)",
                       max_instances=1, coalesce=True)

    # FUB roster sync: every 4 hours (2am, 6am, 10am, 2pm, 6pm, 10pm ET)
    # Detects new agents and sends them the goal setup onboarding email.
    # Lightweight — one FUB API call, updates fub_phone only (never overwrites user-provided phone).
    _scheduler.add_job(scheduled_sync_fub_roster,
                       CronTrigger(hour="2,6,10,14,18,22", minute=15, timezone=ET),
                       id="roster_sync", name="FUB roster sync (every 4h)",
                       max_instances=1, coalesce=True)

    # Daily activity sync: 3:30am + every 2 hours 7am–9pm ET
    # 3:30am: syncs yesterday (complete day) + today (starts fresh)
    # Intraday: updates today's partial counts so agents see live numbers
    # on their dashboard without waiting until tomorrow morning.
    # Uses GREATEST so manual entries that are higher are never overwritten.
    _scheduler.add_job(scheduled_sync_daily_activity,
                       CronTrigger(hour="3,7,9,11,13,15,17,19,21", minute=30, timezone=ET),
                       id="activity_sync", name="FUB daily activity sync (3:30am + intraday)",
                       max_instances=1, coalesce=True)

    # Onboarding escalation: daily 8am ET (Day 3 + Day 7 follow-ups)
    _scheduler.add_job(scheduled_onboarding_escalation,
                       CronTrigger(hour=8, minute=0, timezone=ET),
                       id="onboarding_escalation", name="Onboarding escalation (daily 8am)",
                       max_instances=1, coalesce=True)

    # Gone dark alert: Monday 7am ET
    _scheduler.add_job(scheduled_gone_dark_alert,
                       CronTrigger(day_of_week="mon", hour=7, minute=0, timezone=ET),
                       id="gone_dark", name="Gone dark alert (Mon 7am)",
                       max_instances=1, coalesce=True)

    # Calls cache incremental sync: every 30 minutes
    # Fetches only records newer than the watermark — stays well inside
    # FUB's 2000-record offset cap. Audit/KPI endpoints read from DB cache
    # instead of live FUB so the ISA's high call volume doesn't crowd out agents.
    _scheduler.add_job(scheduled_sync_calls_cache,
                       CronTrigger(minute="*/30", timezone=ET),
                       id="calls_cache_sync", name="Calls cache sync (every 30 min)",
                       max_instances=1, misfire_grace_time=120)

    _scheduler.start()
    print(f"[SCHEDULER] APScheduler started with {len(_scheduler.get_jobs())} jobs:")
    for job in _scheduler.get_jobs():
        print(f"  → {job.name} | next: {job.next_run_time}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    _load_cooldown_state()
    warmup_cache()
    start_scheduler()
    app.run(debug=debug, port=port, host="0.0.0.0")
else:
    # Running under gunicorn
    _load_cooldown_state()
    warmup_cache()
    start_scheduler()
    # Ensure pond email/reply/SMS log tables exist
    _db.ensure_pond_email_log_table()
    _db.ensure_pond_reply_log_table()
    _db.ensure_pond_sms_log_table()
    _db.ensure_pond_sms_reply_log_table()
    # Ensure calls cache table exists (incremental FUB sync)
    _db.ensure_calls_cache_table()
    # Ensure pond mailer job tracking table exists at startup (not just on first run)
    _db.ensure_pond_mailer_jobs_table()
    # Ensure Serendipity Clause tables exist
    _db.ensure_serendipity_tables()
