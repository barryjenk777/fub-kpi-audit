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

# In-memory settings (used when filesystem is read-only, e.g., Railway)
_memory_settings = {}


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

    try:
        data = run_audit_data(weeks_back, min_calls, min_convos, max_ooc)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    """Sales Manager tab — 4 weeks of agent trends + focus list."""
    try:
        client = FUBClient()
        weeks_data = []

        # Fetch 4 weeks of data
        for week_num in range(1, 5):
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            until = today - timedelta(days=7 * (week_num - 1))
            since = until - timedelta(days=7)

            agent_map = auto_detect_agents(client)
            all_calls = client.get_calls(since=since, until=until)
            all_appointments = client.get_appointments(since=since, until=until)

            week_agents = {}
            for name, user in agent_map.items():
                uid = user["id"]
                outbound, convos, talk_secs = count_calls_for_user(all_calls, uid)
                appts_set, appts_met = count_appointments_for_user(all_appointments, uid)
                week_agents[name] = {
                    "calls": outbound,
                    "convos": convos,
                    "appts_set": appts_set,
                    "appts_met": appts_met,
                    "talk_secs": talk_secs,
                }

            week_label = f"{since.strftime('%b %d')} - {(until - timedelta(days=1)).strftime('%b %d')}"
            weeks_data.append({"label": week_label, "agents": week_agents})

        # Build per-agent trend analysis
        agent_names = sorted(weeks_data[0]["agents"].keys())
        agent_trends = []

        for name in agent_names:
            weeks = []
            for wd in weeks_data:
                w = wd["agents"].get(name, {"calls": 0, "convos": 0, "appts_set": 0, "appts_met": 0, "talk_secs": 0})
                weeks.append(w)

            # Current = week 0, previous = week 1
            curr = weeks[0]
            prev = weeks[1] if len(weeks) > 1 else curr

            # Trend calculation
            def trend(curr_val, prev_val):
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

            calls_trend = trend(curr["calls"], prev["calls"])
            convos_trend = trend(curr["convos"], prev["convos"])

            # Concern score: higher = more concerning
            concern = 0
            if curr["calls"] == 0:
                concern += 50
            if calls_trend == "down":
                concern += 30
            if convos_trend == "down":
                concern += 20
            # Declining 2+ weeks in a row
            if len(weeks) >= 3 and weeks[0]["calls"] < weeks[1]["calls"] < weeks[2]["calls"]:
                concern += 25

            # Build insight sentences
            insights = []
            if curr["calls"] == 0:
                insights.append(f"{name} made 0 calls this week.")
            elif calls_trend == "down":
                drop = pct_change(curr["calls"], prev["calls"])
                insights.append(f"Calls dropped {abs(drop)}% from last week ({prev['calls']} → {curr['calls']}).")
            if curr["convos"] == 0 and prev.get("convos", 0) == 0:
                insights.append(f"0 conversations for 2 weeks straight.")
            if convos_trend == "down" and curr["convos"] < prev.get("convos", 0):
                insights.append(f"Conversations declining ({prev['convos']} → {curr['convos']}).")

            agent_trends.append({
                "name": name,
                "current": curr,
                "previous": prev,
                "weeks": [w for w in weeks],  # [current, prev, 2wk ago, 3wk ago]
                "calls_trend": calls_trend,
                "convos_trend": convos_trend,
                "calls_change": pct_change(curr["calls"], prev["calls"]),
                "convos_change": pct_change(curr["convos"], prev["convos"]),
                "concern": concern,
                "insights": insights,
            })

        # Sort by concern (worst first)
        agent_trends.sort(key=lambda a: a["concern"], reverse=True)

        # Team totals per week
        team_weeks = []
        for wd in weeks_data:
            totals = {"calls": 0, "convos": 0, "appts_set": 0}
            for ag in wd["agents"].values():
                totals["calls"] += ag["calls"]
                totals["convos"] += ag["convos"]
                totals["appts_set"] += ag["appts_set"]
            team_weeks.append({"label": wd["label"], "totals": totals})

        # Focus list: bottom third by concern
        focus_count = max(len(agent_trends) // 3, 1)
        focus_list = agent_trends[:focus_count]

        return jsonify({
            "agent_trends": agent_trends,
            "team_weeks": team_weeks,
            "focus_list": focus_list,
            "week_labels": [wd["label"] for wd in weeks_data],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/isa")
def api_isa():
    """ISA Performance tab — Fhalen's metrics, funnel, pipeline, dropped balls."""
    try:
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

        # 4-week sparkline data
        sparkline_calls = []
        sparkline_convos = []
        sparkline_appts = []
        for wk in range(4):
            u = today - timedelta(days=7 * wk)
            s = u - timedelta(days=7)
            wk_calls = client.get_calls(since=s, until=u)
            wk_appts = client.get_appointments(since=s, until=u)
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

        # Pipeline snapshot
        all_leads = client.get_people(assigned_user_id=isa_id, limit=500)
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
            since=today - timedelta(days=30), until=today
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

        return jsonify({
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
        })
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
