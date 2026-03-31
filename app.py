#!/usr/bin/env python3
"""
Legacy Home Team — KPI Audit Dashboard
Flask web app for visualizing agent KPI performance.
"""

import logging
import os
import sys
import json
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
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    until = today
    since = today - timedelta(days=config.AUDIT_PERIOD_DAYS * weeks_back)

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

    return {
        "agents": agents,
        "totals": totals,
        "period": {
            "start": since.strftime("%b %d"),
            "end": (until - timedelta(days=1)).strftime("%b %d, %Y"),
        },
        "thresholds": {
            "min_calls": config.MIN_OUTBOUND_CALLS,
            "min_convos": config.MIN_CONVERSATIONS,
            "max_ooc": config.MAX_OUT_OF_COMPLIANCE,
        },
        "live_calls_admin": getattr(config, "LIVE_CALLS_ADMIN", "Admin"),
        "api_requests": client.request_count,
    }


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

        now = datetime.now(timezone.utc)
        since = now - timedelta(days=config.AUDIT_PERIOD_DAYS)

        # Build results dict in the format email_report expects
        results = {}
        for a in audit_data["agents"]:
            results[a["name"]] = {
                "user": {"id": a["user_id"]},
                "metrics": a["metrics"],
                "evaluation": a["evaluation"],
            }

        success = _send(results, since, now)
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

        # Single fetch covering full 4-week range
        full_since = today - timedelta(days=28)
        all_calls_4w = client.get_calls(since=full_since, until=today)
        all_appts_4w = client.get_appointments(since=full_since, until=today)

        # Split into weekly buckets
        for week_num in range(4):
            until = today - timedelta(days=7 * week_num)
            since = until - timedelta(days=7)
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
                if c.get("userId") != isa_id:
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
        fhalen_out_curr = [c for c in calls_curr if c.get("userId") == isa_id and not c.get("isIncoming")]
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
            if c.get("userId") == isa_id and (c.get("duration", 0) or 0) > 0:
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
        if person_id:
            if person_id not in person_cache:
                try:
                    person_cache[person_id] = client.get_person(person_id)
                except Exception:
                    person_cache[person_id] = {}
            person_data = person_cache[person_id]
            lead_name = person_data.get("name", lead_name)
            tags = person_data.get("tags", []) or []

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
            "source": person_data.get("source", "Unknown"),
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
                   "tags_removed": 0, "tasks_created": 0, "skipped": 0}

        apt_tags = {config.APT_SET_TAG, config.APT_OUTCOME_NEEDED_TAG, config.APT_STALE_TAG}

        for appt in data["appointments"]:
            pid = appt.get("person_id")
            if not pid:
                actions["skipped"] += 1
                continue

            tags = list(appt.get("tags", []))
            has_any_apt_tag = bool(apt_tags & set(tags))
            outcome = appt.get("outcome")
            tier = appt.get("tier")

            # Outcome logged → remove all apt tags
            if outcome:
                if has_any_apt_tag:
                    for t in apt_tags:
                        if t in tags:
                            tags = [x for x in tags if x != t]
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
        data = build_appointment_data()
        success = send_appointment_email(data)
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
    try:
        with open(MANIFEST_FILE) as f:
            manifest = _json.load(f)
    except Exception:
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
                import tempfile as _tempfile
                eng_log_path = os.path.join(_cache_base, "engagement_log.json")
                try:
                    with open(eng_log_path) as f:
                        eng_log = _json.load(f)
                except Exception:
                    eng_log = {}
                eng_log[last_run_str] = {
                    "captured": now.isoformat(),
                    "mode": manifest.get("last_run_mode", "full"),
                    "agents": {
                        name: {"tagged": a["tagged"], "actioned": a["actioned"]}
                        for name, a in agents_out.items()
                    },
                    "pond": {"tagged": len(pond_leads), "actioned": pond_actioned},
                    "total": grand_total,
                }
                # Prune records older than 30 days
                cutoff = (now - _td(days=30)).isoformat()
                eng_log = {k: v for k, v in eng_log.items() if k >= cutoff}
                fd, tmp = _tempfile.mkstemp(dir=_cache_base, suffix=".tmp")
                with os.fdopen(fd, "w") as f:
                    _json.dump(eng_log, f)
                os.replace(tmp, eng_log_path)
            except Exception as e:
                logger.warning("Could not save engagement log: %s", e)

    return jsonify(result)


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
    try:
        with open(eng_log_path) as f:
            eng_log = _json.load(f)
    except Exception:
        return jsonify({"runs": [], "agents": []})

    now = _dt.now(_tz.utc)
    cutoff = (now - _td(days=7)).isoformat()
    recent = {k: v for k, v in eng_log.items() if k >= cutoff}
    sorted_runs = sorted(recent.items(), key=lambda x: x[0])

    all_agents = set()
    for _, rec in sorted_runs:
        all_agents.update(rec.get("agents", {}).keys())
    all_agents = sorted(all_agents)

    runs_out = []
    for run_time, rec in sorted_runs:
        try:
            dt = _dt.fromisoformat(run_time.replace("Z", "+00:00"))
            label = dt.strftime("%-I:%M %p") + "\n" + dt.strftime("%a %-m/%-d")
        except Exception:
            label = run_time[:16]

        agents_data = {}
        for agent in all_agents:
            a = rec.get("agents", {}).get(agent, {"tagged": 0, "actioned": 0})
            t = a.get("tagged", 0)
            ac = a.get("actioned", 0)
            agents_data[agent] = {
                "tagged": t,
                "actioned": ac,
                "rate": round(ac / t * 100) if t > 0 else 0,
            }

        total = rec.get("total", 0)
        total_actioned = sum(a["actioned"] for a in agents_data.values())
        runs_out.append({
            "run_time": run_time,
            "label": label,
            "mode": rec.get("mode", "full"),
            "agents": agents_data,
            "total": {
                "tagged": total,
                "actioned": total_actioned,
                "rate": round(total_actioned / total * 100) if total > 0 else 0,
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
    try:
        with open(MANIFEST_FILE) as f:
            manifest = _json.load(f)
        last_run_str = manifest.get("last_run")
        last_run = _dt.fromisoformat(last_run_str) if last_run_str else None
        if last_run and last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=_tz.utc)
    except Exception:
        last_run = None
        last_run_str = None

    if last_run is None:
        status = "unknown"
        age_hours = None
    else:
        age_hours = (now - last_run).total_seconds() / 3600
        # Scoring runs every 4h — allow up to 5h before flagging stale
        status = "ok" if age_hours <= 5 else "stale"

    # Scheduler status
    scheduler_info = {"running": _scheduler_started}
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        # Get next run times from the scheduler if available
        for var_name, var_val in globals().items():
            if isinstance(var_val, BackgroundScheduler):
                scheduler_info["jobs"] = [
                    {"name": j.name, "next_run": str(j.next_run_time)}
                    for j in var_val.get_jobs()
                ]
                break
    except Exception:
        pass

    # Cache status
    cache_status = {}
    for endpoint in ["audit", "manager", "isa"]:
        cached = cache_get(endpoint)
        if cached:
            cached_at = cached.get("cached_at")
            cache_status[endpoint] = {"cached": True, "cached_at": cached_at}
        else:
            cache_status[endpoint] = {"cached": False}

    return jsonify({
        "status": status,
        "last_run": last_run_str,
        "age_hours": round(age_hours, 1) if age_hours is not None else None,
        "next_run_expected": "every 4 hours at :07 past 6am/10am/2pm/6pm UTC",
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


# ---- Scheduler: cache warming + email delivery ----
_scheduler_started = False


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


def scheduled_cache_warm():
    """Called by APScheduler 3x/day to keep cache fresh."""
    print(f"[SCHEDULER] Cache warm started at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    cache_clear()  # Clear stale data first
    try:
        with app.test_client() as tc:
            tc.get("/api/audit")
            print("[SCHEDULER] Audit cache warmed ✓")
            tc.get("/api/manager")
            print("[SCHEDULER] Manager cache warmed ✓")
            tc.get("/api/isa")
            print("[SCHEDULER] ISA cache warmed ✓")
            tc.get("/api/appointments")
            print("[SCHEDULER] Appointments cache warmed ✓")
    except Exception as e:
        print(f"[SCHEDULER] Cache warm error: {e}")


def scheduled_send_audit_email():
    """Monday 8:30am ET — send KPI audit report."""
    print(f"[SCHEDULER] Sending audit email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-email", json={})
            print(f"[SCHEDULER] Audit email: {resp.data.decode()}")
    except Exception as e:
        print(f"[SCHEDULER] Audit email error: {e}")


def scheduled_send_manager_email():
    """Sunday 3pm ET — send Joe's coaching email."""
    print(f"[SCHEDULER] Sending manager email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-manager-email")
            print(f"[SCHEDULER] Manager email: {resp.data.decode()}")
    except Exception as e:
        print(f"[SCHEDULER] Manager email error: {e}")


def scheduled_sync_appointment_tags():
    """Runs 3x/day — sync APT_SET/APT_OUTCOME_NEEDED/APT_STALE tags."""
    print(f"[SCHEDULER] Syncing appointment tags at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/appointments/sync-tags")
            print(f"[SCHEDULER] Appointment tag sync: {resp.data.decode()}")
    except Exception as e:
        print(f"[SCHEDULER] Appointment tag sync error: {e}")


def scheduled_send_appointment_email():
    """Tuesday 9am ET — send appointment accountability email."""
    print(f"[SCHEDULER] Sending appointment email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-appointment-email")
            print(f"[SCHEDULER] Appointment email: {resp.data.decode()}")
    except Exception as e:
        print(f"[SCHEDULER] Appointment email error: {e}")


def scheduled_send_isa_email():
    """Monday 10am ET — send Fhalen's ISA email."""
    print(f"[SCHEDULER] Sending ISA email at {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    try:
        with app.test_client() as tc:
            resp = tc.post("/api/send-isa-email")
            print(f"[SCHEDULER] ISA email: {resp.data.decode()}")
    except Exception as e:
        print(f"[SCHEDULER] ISA email error: {e}")


def start_scheduler():
    """Start APScheduler with cache warming (3x/day) + email schedules."""
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("[SCHEDULER] APScheduler not installed — skipping scheduled jobs")
        return

    scheduler = BackgroundScheduler(timezone="US/Eastern")

    # Cache warming: 3x/day at 6am, 12pm, 6pm ET
    scheduler.add_job(scheduled_cache_warm, CronTrigger(hour="6,12,18", minute=0),
                      id="cache_warm", name="Cache warm (3x/day)")

    # Joe's coaching email: Sunday 3pm ET
    scheduler.add_job(scheduled_send_manager_email, CronTrigger(day_of_week="sun", hour=15, minute=0),
                      id="manager_email", name="Joe's Sunday coaching email")

    # KPI Audit email: Monday 8:30am ET
    scheduler.add_job(scheduled_send_audit_email, CronTrigger(day_of_week="mon", hour=8, minute=30),
                      id="audit_email", name="Monday KPI audit email")

    # Fhalen ISA email: Monday 10am ET
    scheduler.add_job(scheduled_send_isa_email, CronTrigger(day_of_week="mon", hour=10, minute=0),
                      id="isa_email", name="Monday ISA email")

    # Appointment tag sync: 3x/day at 7am, 1pm, 7pm ET
    scheduler.add_job(scheduled_sync_appointment_tags, CronTrigger(hour="7,13,19", minute=0),
                      id="appt_tag_sync", name="Appointment tag sync (3x/day)")

    # Appointment accountability email: Tuesday 9am ET
    scheduler.add_job(scheduled_send_appointment_email, CronTrigger(day_of_week="tue", hour=9, minute=0),
                      id="appt_email", name="Tuesday appointment email")

    scheduler.start()
    print("[SCHEDULER] APScheduler started with 4 jobs:")
    for job in scheduler.get_jobs():
        print(f"  → {job.name} | next: {job.next_run_time}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    warmup_cache()
    start_scheduler()
    app.run(debug=debug, port=port, host="0.0.0.0")
else:
    # Running under gunicorn
    warmup_cache()
    start_scheduler()
