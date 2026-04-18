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
    """Get cached data for today. Checks memory first, then disk."""
    key = _cache_key(endpoint)
    # Memory cache
    if key in _cache:
        return _cache[key]
    # Disk cache (works locally, not on Railway)
    try:
        path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
                _cache[key] = data  # Promote to memory
                return data
    except Exception:
        pass
    return None


def cache_set(endpoint, data):
    """Store data in today's cache. Memory always, disk when possible."""
    key = _cache_key(endpoint)
    _cache[key] = data
    # Try disk
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
        with open(path, "w") as f:
            json.dump(data, f)
    except OSError:
        pass  # Read-only on Railway


def cache_clear(endpoint=None):
    """Clear cache. If endpoint given, clear just that; otherwise clear all."""
    global _cache
    if endpoint:
        key = _cache_key(endpoint)
        _cache.pop(key, None)
        try:
            path = os.path.join(CACHE_DIR, f"{key.replace(':', '_')}.json")
            os.remove(path)
        except OSError:
            pass
    else:
        _cache.clear()
        try:
            import shutil
            if os.path.exists(CACHE_DIR):
                shutil.rmtree(CACHE_DIR)
        except OSError:
            pass


def load_settings():
    """Load saved KPI thresholds. Priority: memory > file > env vars > defaults."""
    # Memory takes priority (set via Save Settings button)
    if _memory_settings:
        return dict(_memory_settings)
    # Then try settings.json
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Then env vars (set these in Railway for persistence!)
    s = {}
    if os.environ.get("MIN_CALLS"):
        s["min_calls"] = int(os.environ["MIN_CALLS"])
    if os.environ.get("MIN_CONVOS"):
        s["min_convos"] = int(os.environ["MIN_CONVOS"])
    if os.environ.get("MAX_OOC"):
        s["max_ooc"] = int(os.environ["MAX_OOC"])
    return s


def save_settings(min_calls, min_convos, max_ooc):
    """Persist KPI thresholds. Saves to file + memory + env vars."""
    global _memory_settings
    data = {"min_calls": min_calls, "min_convos": min_convos, "max_ooc": max_ooc}
    # Save to memory always (survives within same process)
    _memory_settings = dict(data)
    # Also set env vars (survives within same process on Railway)
    os.environ["MIN_CALLS"] = str(min_calls)
    os.environ["MIN_CONVOS"] = str(min_convos)
    os.environ["MAX_OOC"] = str(max_ooc)
    # Try to save to file (works locally, not on Railway)
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass  # Read-only filesystem (Railway)
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

    # Fetch calls
    all_calls = client.get_calls(since=since, until=until)
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
        speed_avg, speed_count = calculate_speed_to_lead(client, user_id, since)
        violations, ooc_leads, ooc_sphere = count_compliance_violations(
            client, user_id, config.COMPLIANCE_TAG
        )
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
        cw_calls = client.get_calls(since=cw_since2, until=cw_until2) if cw_days_elapsed > 0 else []
        cw_appts_raw = client.get_appointments(since=cw_since2, until=cw_until2) if cw_days_elapsed > 0 else []
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
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


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
                return jsonify(cached)

        data = run_audit_data(weeks_back, min_calls, min_convos, max_ooc)
        data["from_cache"] = False
        data["cached_at"] = datetime.now(timezone.utc).isoformat()

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

        # IMPORTANT: Fetch calls per-agent (not a single bulk fetch) to avoid the
        # 2000-record pagination cap being exhausted by high-volume system accounts.
        # User ID 1 alone logs ~1,400+ calls/week (automated/ISA), filling the cap
        # before any agent calls are reached. Per-agent fetches stay well under 2000.
        all_calls_4w = []
        seen_call_ids: set = set()
        for _name, _user in agent_map.items():
            _uid = _user["id"]
            for _c in client.get_calls(user_id=_uid, since=full_since, until=today):
                _cid = _c.get("id")
                if _cid not in seen_call_ids:
                    seen_call_ids.add(_cid)
                    all_calls_4w.append(_c)

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

        result = {
            "agent_trends": agent_trends,
            "team_weeks": team_weeks,
            "coaching_summary": coaching_summary,
            "kpi": kpi,
            "week_labels": [wd["label"] for wd in weeks_data],
            "from_cache": False,
            "cached_at": datetime.now(timezone.utc).isoformat(),
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

        # Single batch fetch: 4 weeks of calls + appointments
        full_since = today - timedelta(days=28)
        all_calls_4w = client.get_calls(since=full_since, until=today)
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
            call_to_appt_rate=float(body.get("call_to_appt_rate", 0.06)),
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

        return jsonify({"success": True, "goal": goal})
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
            "calls_ytd": a.get("calls_ytd", 0),
            "appts_ytd": a.get("appts_ytd", 0),
            "closings_ytd": deal_summary.get("closings", 0),
        }
    pace = _db.compute_pace(goal, targets, actuals) if goal else {}

    return jsonify({
        "agent_name": agent_name,
        "year": year,
        "goal": goal,
        "why": why,
        "identity": ident,
        "streak": streak,
        "today": today_act,
        "activity_log": activity,
        "targets": targets,
        "actuals": actuals,
        "pace": pace,
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
        "sendgrid_key_present": bool(_os.environ.get("SENDGRID_API_KEY")),
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
        calls_by_uid = {}
        for call in ytd_calls:
            if call.get("isIncoming"):
                continue
            uid = call.get("userId")
            if uid:
                calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1

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
            appts = appts_by_uid.get(fub_uid, 0)
            if _db.upsert_ytd_cache(agent_name, year, calls, appts):
                cached += 1
                details[agent_name] = {"calls_ytd": calls, "appts_ytd": appts}

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
                "actuals":      {"calls_ytd": 0, "appts_ytd": 0,
                                 "contracts_ytd": 0, "closings_ytd": 0, "gci_ytd": 0},
                "pace":         _empty_pace,
                "my_goals_url": _my_goals_url,
            })
            continue

        targets = _db.compute_targets(goal)
        deals   = deal_summaries.get(agent, {"contracts": 0, "closings": 0, "gci_est": 0.0})
        cached  = ytd_cache.get(agent, {"calls_ytd": 0, "appts_ytd": 0})
        actuals = {
            "calls_ytd":     cached["calls_ytd"],
            "appts_ytd":     cached["appts_ytd"],
            "contracts_ytd": deals["contracts"],
            "closings_ytd":  deals["closings"],
            "gci_ytd":       deals["gci_est"],
        }
        pace = _db.compute_pace(goal, targets, actuals)
        scorecard.append({
            "agent_name":   agent,
            "email":        profile.get("email", ""),
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
        dry_run  (bool, default true)  — preview without sending
        person   (int)                 — single FUB person ID for testing
        limit    (int)                 — max leads to process this run
    """
    import threading, uuid
    data     = request.json or {}
    dry_run  = data.get("dry_run", True)
    person   = data.get("person")
    limit    = data.get("limit")

    job_id = str(uuid.uuid4())[:8]

    # Persist job to DB so it survives Railway redeploys
    _db.ensure_pond_mailer_jobs_table()
    _db.create_pond_mailer_job(job_id, dry_run=dry_run)

    def _bg():
        try:
            from pond_mailer import run_pond_mailer
            result = run_pond_mailer(dry_run=dry_run, person_id=person, limit=limit)
            _db.finish_pond_mailer_job(job_id, result=result)
        except Exception as e:
            logger.error("Pond mailer error: %s", e)
            import traceback
            _db.finish_pond_mailer_job(job_id, error=f"{e}\n{traceback.format_exc()}")

    threading.Thread(target=_bg, daemon=True).start()

    return jsonify({"job_id": job_id, "status": "running", "dry_run": dry_run})


@app.route("/api/pond-mailer/status/<job_id>")
def api_pond_mailer_status(job_id):
    job = _db.get_pond_mailer_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.route("/api/pond-mailer/stats")
def api_pond_mailer_stats():
    """Email send history and performance stats."""
    try:
        stats = _db.get_pond_email_stats(days=30)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        person_id, person_name = _db.get_pond_email_person_by_email(clean_from)

        if not person_id:
            logger.info("Pond reply: no matching pond lead for %s", clean_from)
            return jsonify({"status": "ok", "action": "unmatched"}), 200

        logger.info("Pond reply matched: %s (ID: %s)", person_name, person_id)

        # Analyze sentiment
        sentiment, sentiment_score, sentiment_reason = _pond_analyze_sentiment(
            body_text, person_name
        )
        logger.info("Sentiment: %s (%.2f) — %s", sentiment, sentiment_score, sentiment_reason)

        routed   = False
        task_id  = None

        if sentiment == "positive":
            try:
                from fub_client import FUBClient
                from config import MANAGER_USER_ID, PRIORITY_GROUP_ID
                fub = FUBClient()

                # Pull Priority Agents group and pick one agent (deterministic round-robin by person_id)
                group = fub.get_group(PRIORITY_GROUP_ID)
                group_users = [u.get("id") for u in (group.get("users") or []) if u.get("id")]
                if group_users:
                    assigned_uid = group_users[person_id % len(group_users)]
                else:
                    person_data = fub.get_person(person_id)
                    assigned_uid = (
                        person_data.get("assignedUserId")
                        or person_data.get("ownerId")
                        or MANAGER_USER_ID
                    )

                # Assign lead to the chosen Priority Agent (moves them out of pond)
                fub._request("PUT", f"people/{person_id}", json_data={
                    "assignedUserId": assigned_uid,
                })
                logger.info("Lead %s assigned to Priority Agent user %s", person_id, assigned_uid)

                from datetime import date as _date
                tomorrow = (_date.today() + timedelta(days=1)).isoformat()

                task_name = f"\U0001f525 Pond lead replied — call {person_name or clean_from} NOW"
                task_desc = (
                    f"This lead replied to Barry's AI outreach email with POSITIVE interest.\n\n"
                    f"Reply preview:\n{body_text.strip()[:500]}\n\n"
                    f"Original subject: {subject}\n"
                    f"AI read: {sentiment_reason}\n\n"
                    f"They've been assigned to you from the Shark Tank pond. Call them today."
                )

                task_result = fub.create_task(
                    person_id=person_id,
                    assigned_user_id=assigned_uid,
                    name=task_name,
                    due_date=tomorrow,
                    description=task_desc,
                )
                task_id = (task_result or {}).get("id")
                routed = True
                logger.info("FUB task %s created for user %s", task_id, assigned_uid)

                # Generate and add an engaging Claude-written note
                _pond_add_fub_note(fub, person_id, person_name, body_text, subject, sentiment_reason)

            except Exception as e:
                logger.error("FUB routing failed for %s (ID %s): %s", person_name, person_id, e)

        elif sentiment == "negative":
            # Tag lead to suppress from future pond sends
            try:
                from fub_client import FUBClient
                fub = FUBClient()
                fub.add_tag(person_id, "PondMailer_Unsubscribed")
                logger.info("Tagged %s as PondMailer_Unsubscribed", person_name)
            except Exception as e:
                logger.warning("Could not tag unsubscribe for %s: %s", person_name, e)

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

        return jsonify({
            "status":      "ok",
            "person_id":   person_id,
            "person_name": person_name,
            "sentiment":   sentiment,
            "routed":      routed,
            "task_id":     task_id,
        }), 200

    except Exception as e:
        logger.error("Pond reply webhook unhandled error: %s", e, exc_info=True)
        # Always 200 — we don't want SendGrid to retry unrecoverable errors
        return jsonify({"status": "ok", "error": str(e)}), 200


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


def _pond_add_fub_note(fub, person_id, person_name, reply_text, original_subject, sentiment_reason):
    """
    Generate an engaging FUB note using Claude and post it to the lead.
    The note clearly flags this as an AI email reply, summarizes what the
    lead said, and gives the agent a concrete recommended action.
    Falls back to a plain template if Claude is unavailable.
    """
    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        note_body = None

        if api_key:
            try:
                import anthropic, json as _json, re
                client = anthropic.Anthropic(api_key=api_key)

                prompt = f"""Write a short internal CRM note for a real estate agent.

Context:
- Lead name: {person_name or "this lead"}
- They received a personalized AI-written email from Barry Jenkins (Legacy Home Team)
- Subject line of Barry's email: "{original_subject}"
- The lead REPLIED with positive interest
- Their reply: {reply_text.strip()[:800]}
- AI sentiment read: {sentiment_reason}

Write the note AS IF you're Barry briefing the agent who just got this lead assigned to them.
Make it:
- Energetic and direct — this is a hot hand-off
- Clear that the lead responded to an AI-written outreach email (not a cold call)
- A 2-3 sentence gist of what the lead actually said in their reply
- One specific recommended action step the agent should take on the phone call
- Under 200 words total
- No fluff, no "I hope this finds you well" — get to the point

Output plain text only. No JSON, no markdown headers."""

                response = client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}],
                )
                note_body = response.content[0].text.strip()
            except Exception as e:
                logger.warning("Claude note generation failed: %s", e)

        # Fallback if Claude unavailable or fails
        if not note_body:
            note_body = (
                f"🔥 HOT HAND-OFF — {person_name or 'Lead'} replied to Barry's AI outreach email.\n\n"
                f"Email subject: \"{original_subject}\"\n\n"
                f"What they said:\n{reply_text.strip()[:600]}\n\n"
                f"AI read: {sentiment_reason}\n\n"
                f"Recommended action: Call them today. Open with: "
                f"\"Hey, I saw you replied to Barry's email — I wanted to reach out personally.\""
            )

        fub._request("POST", "notes", json_data={
            "personId": person_id,
            "body":     note_body,
        })
        logger.info("FUB note added for person %s", person_id)
    except Exception as e:
        logger.warning("Could not add FUB note for reply (person %s): %s", person_id, e)


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
        all_calls = fub.get_calls(since=d_start_utc, until=d_end_utc)
        calls_by_uid = {}
        for c in all_calls:
            uid = c.get("userId")
            if uid:
                calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1

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
            calls = calls_by_uid.get(fub_uid, 0)
            appts = appts_by_uid.get(fub_uid, 0)
            if calls > 0 or appts > 0:
                _db.upsert_daily_activity_fub(agent["agent_name"], target_date, calls, appts)
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

            calls_by_uid  = {}
            for call in ytd_calls:
                if call.get("isIncoming"):
                    continue
                uid = call.get("userId")
                if uid:
                    calls_by_uid[uid] = calls_by_uid.get(uid, 0) + 1

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
                appts = appts_by_uid.get(fub_uid, 0)
                if _db.upsert_ytd_cache(agent_name, year, calls, appts):
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


def scheduled_pond_mailer():
    """
    Daily job (Mon-Fri 9:15am ET): send up to 10 personalized emails to
    Shark Tank pond leads based on their IDX browsing behavior.
    Emails are written by Claude, sent via SendGrid.
    """
    if not _db.try_acquire_job_lock("pond_mailer"):
        return
    try:
        from pond_mailer import run_pond_mailer
        result = run_pond_mailer(dry_run=False)
        sent = result.get("sent", 0)
        print(f"[POND MAILER] Run complete — {sent} emails sent")
    except Exception as e:
        _alert_on_job_failure("pond_mailer", str(e))
    finally:
        _db.release_job_lock("pond_mailer")


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

    # Pond mailer: 3x daily Mon–Fri (10am, 2pm, 6pm ET)
    # 9 leads per run × 3 runs = 27/day max — safe for a warmed domain.
    # Running 3x catches leads within ~1-2 hours of their IDX activity.
    for _pm_hour, _pm_min, _pm_label in [(10, 0, "10am"), (14, 0, "2pm"), (18, 0, "6pm")]:
        _scheduler.add_job(scheduled_pond_mailer,
                           CronTrigger(day_of_week="mon-fri", hour=_pm_hour,
                                       minute=_pm_min, timezone=ET),
                           id=f"pond_mailer_{_pm_hour}",
                           name=f"Pond mailer (Mon-Fri {_pm_label} ET)",
                           max_instances=1, coalesce=True)

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
    # Ensure pond email/reply log tables exist
    _db.ensure_pond_email_log_table()
    _db.ensure_pond_reply_log_table()
