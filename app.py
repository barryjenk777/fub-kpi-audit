#!/usr/bin/env python3
"""
Legacy Home Team — KPI Audit Dashboard
Flask web app for visualizing agent KPI performance.
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone

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
                return jsonify(cached)

        data = run_audit_data(weeks_back, min_calls, min_convos, max_ooc)
        data["from_cache"] = False

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


# ---- LeadStream: Dashboard ----

_ls_dashboard_cache = {"data": None, "time": None}

@app.route("/leadstream")
def leadstream_dashboard():
    return render_template("leadstream.html")


@app.route("/api/leadstream/dashboard")
def api_leadstream_dashboard():
    """Return LeadStream status, current tagged leads, and activity since last run."""
    import json as _json
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    # 3-minute cache to avoid hammering FUB API on every page refresh
    now = _dt.now(_tz.utc)
    cached = _ls_dashboard_cache
    if cached["data"] and cached["time"] and (now - cached["time"]).seconds < 180:
        return jsonify(cached["data"])

    MANIFEST_FILE = os.path.join(os.path.dirname(__file__), ".cache", "leadstream_manifest.json")
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

    # Fetch activity since last run (3 bulk API calls)
    calls_by_person = {}
    texts_by_person = {}
    updated_person_ids = set()

    if last_run:
        try:
            _client = FUBClient()
            since = last_run - _td(minutes=10)

            try:
                for call in _client.get_calls(since=since):
                    if not call.get("isIncoming"):
                        pid = call.get("personId")
                        if pid:
                            calls_by_person[pid] = calls_by_person.get(pid, 0) + 1
            except Exception:
                pass

            try:
                for text in _client.get_text_messages(since=since):
                    if text.get("isOutbound"):
                        pid = text.get("personId")
                        if pid:
                            texts_by_person[pid] = texts_by_person.get(pid, 0) + 1
            except Exception:
                pass

            try:
                for person in _client.get_people(updated_since=since):
                    pid = person.get("id")
                    if pid:
                        updated_person_ids.add(pid)
            except Exception:
                pass
        except Exception:
            pass

    def _enrich(item):
        pid = item["id"] if isinstance(item, dict) else item
        called = calls_by_person.get(pid, 0) > 0
        texted = texts_by_person.get(pid, 0) > 0
        updated = pid in updated_person_ids
        return {
            "id": pid,
            "name": item.get("name", f"ID:{pid}") if isinstance(item, dict) else f"ID:{pid}",
            "score": item.get("score", 0) if isinstance(item, dict) else 0,
            "tier": item.get("tier", "") if isinstance(item, dict) else "",
            "stage": item.get("stage", "") if isinstance(item, dict) else "",
            "called": called,
            "texted": texted,
            "updated": updated,
            "actioned": called or texted,
        }

    agents_out = {}
    for agent_name, lead_items in manifest.get("agent", {}).items():
        enriched = [_enrich(item) for item in lead_items]
        actioned = sum(1 for l in enriched if l["actioned"])
        agents_out[agent_name] = {
            "leads": enriched,
            "tagged": len(enriched),
            "actioned": actioned,
        }

    pond_enriched = [_enrich(item) for item in manifest.get("pond", [])]
    pond_actioned = sum(1 for l in pond_enriched if l["actioned"])

    total_agent = sum(a["tagged"] for a in agents_out.values())
    total_actioned = sum(a["actioned"] for a in agents_out.values()) + pond_actioned

    result = {
        "last_run": last_run_str,
        "last_run_mode": manifest.get("last_run_mode", "full"),
        "run_history": manifest.get("run_history", []),
        "agents": agents_out,
        "pond": {
            "leads": pond_enriched,
            "tagged": len(pond_enriched),
            "actioned": pond_actioned,
        },
        "totals": {
            "agent_leads": total_agent,
            "pond_leads": len(pond_enriched),
            "total": total_agent + len(pond_enriched),
            "actioned": total_actioned,
            "action_rate": round(total_actioned / (total_agent + len(pond_enriched)) * 100, 1) if (total_agent + len(pond_enriched)) > 0 else 0,
        },
    }

    _ls_dashboard_cache["data"] = result
    _ls_dashboard_cache["time"] = now
    return jsonify(result)


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
    """Run LeadStream scoring and tag leads."""
    data = request.json or {}
    dry_run = data.get("dry_run", False)
    agent_name = data.get("agent", None)
    pond_only = data.get("pond_only", False)

    try:
        from lead_scoring import LeadScorer, _get_leadstream_client
        client = _get_leadstream_client()
        scorer = LeadScorer(client)
        results = scorer.run(dry_run=dry_run, agent_name=agent_name, pond_only=pond_only)

        return jsonify({
            "success": True,
            "dry_run": dry_run,
            "agents": {
                name: {
                    "count": info["count"],
                    "leads": [
                        {"name": l["name"], "score": l["score"], "tier": l["tier"]}
                        for l in info["leads"]
                    ],
                }
                for name, info in results.get("agents", {}).items()
            },
            "pond": [
                {"name": l["name"], "score": l["score"], "tier": l["tier"]}
                for l in results.get("pond", [])
            ],
            "tags_removed": results.get("removed", 0),
            "api_requests": client.request_count,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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


def warmup_cache():
    """Pre-populate cache on startup so first page load is instant."""
    import threading
    def _warmup():
        try:
            with app.test_client() as tc:
                print("[WARMUP] Pre-loading audit data...")
                tc.get("/api/audit")
                print("[WARMUP] Audit cached ✓")
                tc.get("/api/manager")
                print("[WARMUP] Manager cached ✓")
                # ISA is heaviest — do it last
                tc.get("/api/isa")
                print("[WARMUP] ISA cached ✓")
        except Exception as e:
            print(f"[WARMUP] Error: {e}")
    t = threading.Thread(target=_warmup, daemon=True)
    t.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    warmup_cache()
    app.run(debug=debug, port=port, host="0.0.0.0")
else:
    # Running under gunicorn
    warmup_cache()
