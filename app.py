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
    """Load saved KPI thresholds from file, env vars, or memory."""
    # Memory takes priority (set via Save Settings button)
    if _memory_settings:
        return dict(_memory_settings)
    # Then try settings.json
    try:
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Then env vars
    s = {}
    if os.environ.get("MIN_CALLS"):
        s["min_calls"] = int(os.environ["MIN_CALLS"])
    if os.environ.get("MIN_CONVOS"):
        s["min_convos"] = int(os.environ["MIN_CONVOS"])
    if os.environ.get("MAX_OOC"):
        s["max_ooc"] = int(os.environ["MAX_OOC"])
    return s


def save_settings(min_calls, min_convos, max_ooc):
    """Persist KPI thresholds. Tries file first, falls back to memory."""
    global _memory_settings
    data = {"min_calls": min_calls, "min_convos": min_convos, "max_ooc": max_ooc}
    # Save to memory always (works on Railway)
    _memory_settings = dict(data)
    # Try to save to file (works locally)
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

        # Re-run audit with provided thresholds
        audit_data = run_audit_data(
            weeks_back=1,
            min_calls=data.get("min_calls"),
            min_convos=data.get("min_convos"),
            max_ooc=data.get("max_ooc"),
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
                if week_num == 0:
                    try:
                        texts_out, _ = client.count_texts_for_user(uid, since=since, until=until)
                    except Exception:
                        texts_out = 0

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


@app.route("/api/isa")
def api_isa():
    """ISA Performance tab — Fhalen's metrics, funnel, pipeline, dropped balls."""
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

        # Current week
        until_curr = today
        since_curr = today - timedelta(days=7)
        # Previous week
        until_prev = since_curr
        since_prev = since_curr - timedelta(days=7)

        # Fetch data for both weeks
        calls_curr = client.get_calls(since=since_curr, until=until_curr)
        calls_prev = client.get_calls(since=since_prev, until=until_prev)
        appts_curr = client.get_appointments(since=since_curr, until=until_curr)
        appts_prev = client.get_appointments(since=since_prev, until=until_prev)

        def isa_calls(all_calls):
            outbound = 0
            convos = 0
            talk_secs = 0
            for c in all_calls:
                if c.get("userId") != isa_id:
                    continue
                dur = c.get("duration", 0) or 0
                if not c.get("isIncoming", False):
                    outbound += 1
                if dur >= config.CONVERSATION_THRESHOLD_SECONDS:
                    convos += 1
                    talk_secs += dur
            return outbound, convos, talk_secs

        def isa_appts(all_appts):
            appt_set = 0
            appt_met = 0
            no_show = 0
            pending = 0
            for a in all_appts:
                invitees = a.get("invitees", [])
                isa_involved = any(inv.get("userId") == isa_id for inv in invitees)
                has_lead = any(inv.get("personId") for inv in invitees)
                if not isa_involved or not has_lead:
                    continue
                appt_set += 1
                outcome = a.get("outcome")
                if outcome == "Met with Client":
                    appt_met += 1
                elif outcome == "No show":
                    no_show += 1
                elif outcome is None:
                    pending += 1
            return appt_set, appt_met, no_show, pending

        out_curr, conv_curr, talk_curr = isa_calls(calls_curr)
        out_prev, conv_prev, talk_prev = isa_calls(calls_prev)
        appt_set_curr, appt_met_curr, no_show_curr, pending_curr = isa_appts(appts_curr)
        appt_set_prev, appt_met_prev, no_show_prev, pending_prev = isa_appts(appts_prev)

        show_rate_curr = round(appt_met_curr / appt_set_curr * 100) if appt_set_curr > 0 else 0
        show_rate_prev = round(appt_met_prev / appt_set_prev * 100) if appt_set_prev > 0 else 0
        calls_per_appt = round(out_curr / appt_set_curr) if appt_set_curr > 0 else 0

        # 4-week sparkline data — single batch fetch
        full_since = today - timedelta(days=28)
        all_calls_4w = client.get_calls(since=full_since, until=today)
        all_appts_4w = client.get_appointments(since=full_since, until=today)

        sparkline_calls = []
        sparkline_convos = []
        sparkline_appts = []
        for wk in range(4):
            u = today - timedelta(days=7 * wk)
            s = u - timedelta(days=7)
            s_str = s.strftime("%Y-%m-%dT%H:%M:%SZ")
            u_str = u.strftime("%Y-%m-%dT%H:%M:%SZ")
            wk_calls = [c for c in all_calls_4w if s_str <= (c.get("created") or "") < u_str]
            wk_appts = [a for a in all_appts_4w if s_str <= (a.get("start") or a.get("created") or "") < u_str]
            o, cv, _ = isa_calls(wk_calls)
            a_set, _, _, _ = isa_appts(wk_appts)
            label = f"{s.strftime('%b %d')}"
            sparkline_calls.append({"label": label, "value": o})
            sparkline_convos.append({"label": label, "value": cv})
            sparkline_appts.append({"label": label, "value": a_set})

        # Reverse so oldest is first
        sparkline_calls.reverse()
        sparkline_convos.reverse()
        sparkline_appts.reverse()

        # Pipeline snapshot (limit 200 to save memory)
        all_leads = client.get_people(assigned_user_id=isa_id, limit=200)
        stages = {}
        sources = {}
        for p in all_leads:
            stage = p.get("stage") or "Unknown"
            stages[stage] = stages.get(stage, 0) + 1
            source = p.get("source") or "Unknown"
            sources[source] = sources.get(source, 0) + 1

        # Top sources sorted by count
        top_sources = sorted(sources.items(), key=lambda x: x[1], reverse=True)[:6]

        # Speed to lead for ISA
        from kpi_audit import calculate_speed_to_lead
        stl_avg, stl_count = calculate_speed_to_lead(client, isa_id, since_curr)

        # Dropped balls: appointments Fhalen set where agent hasn't followed up
        dropped_balls = []
        all_appts_recent = client.get_appointments(
            since=today - timedelta(days=14), until=today
        )
        for appt in all_appts_recent:
            invitees = appt.get("invitees", [])
            isa_involved = any(inv.get("userId") == isa_id for inv in invitees)
            if not isa_involved:
                continue

            # Find the lead in this appointment
            lead_inv = next((inv for inv in invitees if inv.get("personId")), None)
            if not lead_inv:
                continue

            # Check if outcome is missing (agent didn't log it)
            if appt.get("outcome") is not None:
                continue  # Outcome was logged, not a dropped ball

            person_id = lead_inv["personId"]
            lead_name = lead_inv.get("name", "Unknown")

            # Find which agent this lead is assigned to
            try:
                person = client.get_person(person_id)
                assigned_to = person.get("assignedTo", "Unassigned")
                assigned_uid = person.get("assignedUserId")
                stage = person.get("stage", "Unknown")

                # Skip if assigned to ISA herself
                if assigned_uid == isa_id:
                    continue

                dropped_balls.append({
                    "lead_name": lead_name,
                    "person_id": person_id,
                    "agent_name": assigned_to,
                    "appt_date": appt.get("start", "")[:10],
                    "appt_title": appt.get("title", ""),
                    "stage": stage,
                    "has_tag": config.DROPPED_BALL_TAG.lower() in [
                        t.lower() for t in (person.get("tags") or [])
                    ],
                })
            except Exception:
                continue

        result = {
            "current": {
                "calls": out_curr, "convos": conv_curr,
                "talk_secs": talk_curr, "appts_set": appt_set_curr,
                "appts_met": appt_met_curr, "no_show": no_show_curr,
                "pending": pending_curr, "show_rate": show_rate_curr,
                "calls_per_appt": calls_per_appt,
            },
            "previous": {
                "calls": out_prev, "convos": conv_prev,
                "talk_secs": talk_prev, "appts_set": appt_set_prev,
                "appts_met": appt_met_prev, "show_rate": show_rate_prev,
            },
            "sparkline": {
                "calls": sparkline_calls,
                "convos": sparkline_convos,
                "appts": sparkline_appts,
            },
            "pipeline": {
                "total": len(all_leads),
                "stages": stages,
                "top_sources": [{"source": s, "count": c} for s, c in top_sources],
            },
            "speed_to_lead": {"avg": stl_avg, "count": stl_count},
            "dropped_balls": dropped_balls,
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug, port=port, host="0.0.0.0")
