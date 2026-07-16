"""
Send internal team emails via Postmark.
Designed as a team leader weekly update with actionable insights.

Requires:
  POSTMARK_API_KEY  — Server API Token from postmarkapp.com
                      (Settings → Servers → Legacy Internal → API Tokens)
"""

import os
import random
from datetime import datetime, timedelta

import config
import postmark_client as _pm


def _catchy_subject(email_type, data=None):
    """Generate unique, catchy subject lines so Gmail won't thread them together."""
    week_of = datetime.now().strftime("%b %d")

    if email_type == "audit":
        passed = data.get("passed", 0) if data else 0
        total = data.get("total", 0) if data else 0
        subjects = [
            f"🏆 Legacy Scoreboard — {passed}/{total} Agents Earned Their Spot — Week of {week_of}",
            f"📊 Who's Earning Leads This Week? {passed}/{total} Made the Cut — {week_of}",
            f"⚡ Weekly KPI Drop: {passed}/{total} Agents Qualify for Priority — {week_of}",
            f"🎯 The Leaderboard Is In — {passed}/{total} Agents Hit KPIs — {week_of}",
        ]
    elif email_type == "manager":
        meeting = data.get("meeting", 0) if data else 0
        total = data.get("total", 0) if data else 0
        subjects = [
            f"🎯 Joe's Sunday Playbook — {meeting}/{total} at KPI — Here's Your Game Plan for {week_of}",
            f"🏈 Coaching Blueprint: Who Needs You Most This Week — {week_of}",
            f"📋 Your Pre-Game Scouting Report — {meeting}/{total} Hitting KPIs — {week_of}",
            f"💪 Time to Level Up the Team — {meeting}/{total} at Standard — {week_of}",
        ]
    elif email_type == "isa":
        calls = data.get("calls", 0) if data else 0
        convos = data.get("convos", 0) if data else 0
        subjects = [
            f"📞 ISA Performance Pulse — {calls} Calls, {convos} Conversations — Week of {week_of}",
            f"🔍 Fhalen's Weekly Breakdown: Calls to Closings — {week_of}",
            f"📈 ISA ROI Check — Are We Getting Appointments? — {week_of}",
            f"⚡ Inside Sales Scorecard: Conversion Funnel for {week_of}",
        ]
    elif email_type == "appointments":
        no_outcome = data.get("no_outcome", 0) if data else 0
        total = data.get("total", 0) if data else 0
        completion = data.get("completion_rate", 0) if data else 0
        subjects = [
            f"📅 Appointment Check-In: {no_outcome} Missing Outcomes — {completion}% Complete — {week_of}",
            f"🔔 Are We Closing the Loop? {no_outcome}/{total} Appointments Need Updates — {week_of}",
            f"📋 Appointment Accountability: {completion}% Completion Rate — {week_of}",
            f"⚡ {no_outcome} Appointments Falling Through the Cracks — {week_of}",
        ]
    else:
        subjects = [f"Legacy Home Team Update — {week_of}"]

    return random.choice(subjects)


def _rank_agents(results):
    """Rank agents by overall performance score for leaderboard."""
    scores = []
    for name, data in results.items():
        m = data["metrics"]
        e = data["evaluation"]
        # Weighted score: calls + convos*5 + appts_set*10 + appts_met*20 - ooc
        score = (
            m["outbound_calls"]
            + m["conversations"] * 5
            + m["appts_set"] * 10
            + m["appts_met"] * 20
            - m["compliance_violations"]
        )
        passed_count = sum([
            e["calls_pass"], e["convos_pass"], e["compliance_pass"]
        ])
        scores.append((name, score, passed_count, m))
    return sorted(scores, key=lambda x: x[1], reverse=True)


def _fmt_talk_time(seconds):
    """Format seconds into human-readable talk time."""
    if not seconds:
        return "0m"
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {mins}m"
    return f"{mins}m"


def build_html_report(results, period_start, period_end):
    """Build a team leader weekly update HTML email."""
    passed = sorted(n for n, d in results.items() if d["evaluation"]["overall_pass"])
    failed = sorted(n for n, d in results.items() if not d["evaluation"]["overall_pass"])
    ranked = _rank_agents(results)

    period = f"{period_start.strftime('%b %d')} — {period_end.strftime('%b %d, %Y')}"

    # Team-wide totals
    total_calls = sum(d["metrics"]["outbound_calls"] for d in results.values())
    total_convos = sum(d["metrics"]["conversations"] for d in results.values())
    total_talk = sum(d["metrics"]["talk_time_seconds"] for d in results.values())
    total_appts_set = sum(d["metrics"]["appts_set"] for d in results.values())
    total_appts_met = sum(d["metrics"]["appts_met"] for d in results.values())
    total_ooc = sum(d["metrics"]["compliance_violations"] for d in results.values())

    admin = getattr(config, "LIVE_CALLS_ADMIN", "Admin")

    html = f"""
    <html>
    <head><style>
        body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; max-width: 720px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #1a1a2e; font-size: 22px; border-bottom: 3px solid #0f3460; padding-bottom: 8px; margin-bottom: 4px; }}
        h2 {{ color: #0f3460; font-size: 17px; margin-top: 28px; margin-bottom: 8px; }}
        h3 {{ color: #16213e; font-size: 14px; margin: 20px 0 6px; text-transform: uppercase; letter-spacing: 1px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 8px 0 16px; font-size: 14px; }}
        th {{ background: #0f3460; color: white; padding: 9px 10px; text-align: left; font-size: 13px; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #e8e8e8; }}
        tr:nth-child(even) {{ background: #f8f9fb; }}
        .pass {{ color: #28a745; font-weight: 600; }}
        .fail {{ color: #dc3545; font-weight: 600; }}
        .warn {{ color: #e67e22; font-weight: 600; }}
        .muted {{ color: #999; }}
        .action-box {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px; padding: 14px 18px; margin: 14px 0; }}
        .action-box.green {{ background: #d4edda; border-color: #28a745; }}
        .action-box.red {{ background: #f8d7da; border-color: #dc3545; }}
        .stat-grid {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0; }}
        .stat-card {{ background: #f0f4f8; border-radius: 8px; padding: 14px 18px; flex: 1; min-width: 100px; text-align: center; }}
        .stat-card .num {{ font-size: 28px; font-weight: 700; color: #0f3460; }}
        .stat-card .label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
        .medal {{ font-size: 16px; }}
        .footer {{ color: #888; font-size: 11px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; }}
        .tag {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 12px; font-weight: 600; }}
        .tag-pass {{ background: #d4edda; color: #155724; }}
        .tag-fail {{ background: #f8d7da; color: #721c24; }}
    </style></head>
    <body>

    <div style="background:#0f172a;border-radius:10px;padding:20px 24px;margin-bottom:16px;text-align:center">
      <img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="140" style="display:block;margin:0 auto 10px;width:140px;height:auto">
      <h1 style="margin:0;color:#ffffff;font-size:18px">Weekly Team Update</h1>
      <p style="margin:4px 0 0;color:rgba(255,255,255,0.6);font-size:13px">KPI Window: {period} &nbsp;|&nbsp; Calls &ge;{config.MIN_OUTBOUND_CALLS} &bull; Convos &ge;{config.MIN_CONVERSATIONS} &bull; OOC &le;{config.MAX_OUT_OF_COMPLIANCE}</p>
    </div>
    """

    # ---- Team Snapshot ----
    html += """<h2>Team Snapshot</h2>
    <div class="stat-grid">"""
    html += f'<div class="stat-card"><div class="num">{total_calls}</div><div class="label">Calls Made</div></div>'
    html += f'<div class="stat-card"><div class="num">{total_convos}</div><div class="label">Conversations</div></div>'
    html += f'<div class="stat-card"><div class="num">{_fmt_talk_time(total_talk)}</div><div class="label">Talk Time</div></div>'
    html += f'<div class="stat-card"><div class="num">{total_appts_set}</div><div class="label">Appts Set</div></div>'
    html += f'<div class="stat-card"><div class="num">{total_appts_met}</div><div class="label">Appts Met</div></div>'
    html += f'<div class="stat-card"><div class="num">{len(passed)}/{len(results)}</div><div class="label">KPI Pass</div></div>'
    html += "</div>"

    # ---- Action Items (top of email for quick scanning) ----
    # Compute the routing week (Mon after measured Saturday → following Sunday)
    routing_week_start = period_end + timedelta(days=1)  # Monday after the Sun boundary
    routing_week_end   = routing_week_start + timedelta(days=6)
    measured_label = (
        f"{period_start.strftime('%a %b %-d')} – "
        f"{(period_end - timedelta(days=1)).strftime('%a %b %-d, %Y')}"
    )
    routing_label = (
        f"{routing_week_start.strftime('%b %-d')} – "
        f"{routing_week_end.strftime('%b %-d, %Y')}"
    )

    html += '<h2>Action Items This Week</h2>'

    # Live Calls inbox — with explicit calendar context for Fhalen
    html += f"""
    <div class="action-box">
        <strong>@{admin}:</strong> Please update the <strong>Live Calls</strong> inbox and Priority Group in Follow Up Boss.<br>
        <div style="margin:8px 0 12px;padding:8px 12px;background:#fff8e1;border-left:4px solid #f59e0b;border-radius:4px;font-size:13px">
            📅 <strong>Routing calendar block:</strong> Activity measured
            <strong>{measured_label}</strong> (Mon–Sat work week)<br>
            🗓 <strong>This routing applies for the week of {routing_label}</strong>
        </div>
    """
    if passed:
        html += "<strong>ADD</strong> these agents to Live Calls &amp; Priority Group:<br>"
        for name in passed:
            html += f'&nbsp;&nbsp;&nbsp;&nbsp;&#9989; {name}<br>'
    if failed:
        if passed:
            html += "<br>"
        html += "<strong>REMOVE</strong> these agents from Live Calls &amp; Priority Group:<br>"
        for name in failed:
            html += f'&nbsp;&nbsp;&nbsp;&nbsp;&#10060; {name}<br>'
    html += "</div>"

    # Priority group status
    if passed:
        html += f"""
        <div class="action-box green">
            <strong>Priority Agents Group — Week of {routing_label}:</strong><br>
            {', '.join(passed)}
        </div>"""
    else:
        html += f"""
        <div class="action-box red">
            <strong>Priority Agents Group — Week of {routing_label}:</strong> No agents qualified. All new leads go to the pond.
        </div>"""

    # OOC action items
    if total_ooc > 0:
        top_ooc = sorted(results.items(), key=lambda x: x[1]["metrics"]["compliance_violations"], reverse=True)
        worst = top_ooc[0]
        html += f"""
        <div class="action-box red">
            <strong>Compliance Alert:</strong> {total_ooc} total leads out of compliance across the team.
            {worst[0]} has the most at {worst[1]["metrics"]["compliance_violations"]}.
            All agents need to clear their <code>MAV_NUDGE_OUTSTANDING</code> tags.
        </div>"""

    # ---- Leaderboard ----
    html += '<h2>Agent Leaderboard</h2>'
    html += '<table><tr><th>#</th><th>Agent</th><th>Calls</th><th>Convos</th><th>Appt Set</th><th>Appt Met</th><th>Talk Time</th><th>OOC</th><th>KPI</th></tr>'

    medals = ["&#129351;", "&#129352;", "&#129353;"]  # gold, silver, bronze
    for i, (name, score, passed_count, m) in enumerate(ranked):
        e = results[name]["evaluation"]
        rank_icon = medals[i] if i < 3 else str(i + 1)
        status_tag = '<span class="tag tag-pass">PASS</span>' if e["overall_pass"] else '<span class="tag tag-fail">FAIL</span>'
        talk = _fmt_talk_time(m["talk_time_seconds"])

        ooc_str = str(m["compliance_violations"])
        ooc_cls = ' class="fail"' if not e["compliance_pass"] else ""

        html += f"""
        <tr>
            <td class="medal">{rank_icon}</td>
            <td><strong>{name}</strong></td>
            <td>{m['outbound_calls']}</td>
            <td>{m['conversations']}</td>
            <td>{m['appts_set']}</td>
            <td>{m['appts_met']}</td>
            <td>{talk}</td>
            <td{ooc_cls}>{ooc_str}</td>
            <td>{status_tag}</td>
        </tr>"""

    html += "</table>"

    # ---- Conversion Funnel ----
    html += """
    <h2>Conversion Funnel</h2>
    <table>
    <tr><th>Agent</th><th>Calls</th><th>&rarr; Convos</th><th>&rarr; Appt Set</th><th>&rarr; Appt Met</th><th>Call&rarr;Convo</th><th>Convo&rarr;Appt</th><th>Set&rarr;Met</th></tr>
    """

    for name, data in sorted(results.items()):
        m = data["metrics"]
        calls = m["outbound_calls"]
        convos = m["conversations"]
        appts_set = m["appts_set"]
        appts_met = m["appts_met"]

        call_to_convo = f"{(convos / calls * 100):.0f}%" if calls > 0 else "—"
        convo_to_appt = f"{(appts_set / convos * 100):.0f}%" if convos > 0 else "—"
        set_to_met = f"{(appts_met / appts_set * 100):.0f}%" if appts_set > 0 else "—"

        html += f"""
        <tr>
            <td>{name}</td>
            <td>{calls}</td>
            <td>{convos}</td>
            <td>{appts_set}</td>
            <td>{appts_met}</td>
            <td>{call_to_convo}</td>
            <td>{convo_to_appt}</td>
            <td>{set_to_met}</td>
        </tr>"""

    html += "</table>"

    # ---- OOC Breakdown ----
    html += '<h2>Out of Compliance Breakdown</h2>'

    any_ooc = False
    html += '<table><tr><th>Agent</th><th>Total OOC</th><th>Leads</th><th>Sphere</th></tr>'
    for name, data in sorted(results.items(), key=lambda x: x[1]["metrics"]["compliance_violations"], reverse=True):
        m = data["metrics"]
        total = m["compliance_violations"]
        if total == 0:
            continue
        any_ooc = True
        ooc_cls = ' class="fail"' if total > config.MAX_OUT_OF_COMPLIANCE else ""
        html += f"""
        <tr>
            <td>{name}</td>
            <td{ooc_cls}><strong>{total}</strong></td>
            <td>{m['ooc_leads']}</td>
            <td>{m['ooc_sphere']}</td>
        </tr>"""

    if not any_ooc:
        html += '<tr><td colspan="4" style="text-align:center; color:#28a745;">All agents are in compliance!</td></tr>'
    html += "</table>"

    # ---- Speed to Lead ----
    html += '<h2>Speed to Lead</h2>'
    has_stl = any(d["metrics"]["speed_to_lead_avg"] is not None for d in results.values())
    if has_stl:
        html += '<table><tr><th>Agent</th><th>Avg Response</th><th>Leads Measured</th></tr>'
        for name, data in sorted(results.items()):
            m = data["metrics"]
            if m["speed_to_lead_avg"] is not None:
                stl = m["speed_to_lead_avg"]
                stl_cls = ' class="pass"' if stl <= 5 else (' class="warn"' if stl <= 15 else ' class="fail"')
                html += f'<tr><td>{name}</td><td{stl_cls}>{stl:.1f} min</td><td>{m["speed_to_lead_count"]}</td></tr>'
            else:
                html += f'<tr><td>{name}</td><td class="muted">No new leads</td><td>—</td></tr>'
        html += "</table>"
    else:
        html += '<p class="muted">No new leads assigned this week to measure speed-to-lead.</p>'

    # ---- KPI Details ----
    html += """
    <h2>KPI Scorecard Detail</h2>
    <table>
    <tr><th>Agent</th><th>Calls</th><th>Convos</th><th>STL</th><th>OOC</th><th>Status</th></tr>
    """

    for name, data in sorted(results.items()):
        m = data["metrics"]
        e = data["evaluation"]

        calls_cls = "" if e["calls_pass"] else ' class="fail"'
        convos_cls = "" if e["convos_pass"] else ' class="fail"'
        comp_cls = "" if e["compliance_pass"] else ' class="fail"'
        status_tag = '<span class="tag tag-pass">PASS</span>' if e["overall_pass"] else '<span class="tag tag-fail">FAIL</span>'

        stl = f"{m['speed_to_lead_avg']:.1f}m" if m["speed_to_lead_avg"] is not None else "n/a"

        html += f"""
        <tr>
            <td><strong>{name}</strong></td>
            <td{calls_cls}>{m['outbound_calls']}</td>
            <td{convos_cls}>{m['conversations']}</td>
            <td>{stl}</td>
            <td{comp_cls}>{m['compliance_violations']}</td>
            <td>{status_tag}</td>
        </tr>"""

    html += f"""
    </table>
    <p style="font-size:12px; color:#999;">Thresholds: Calls &ge;{config.MIN_OUTBOUND_CALLS} &bull; Convos &ge;{config.MIN_CONVERSATIONS} &bull; OOC &le;{config.MAX_OUT_OF_COMPLIANCE}</p>
    """

    # Footer
    html += f"""
    <p class="footer">
        Legacy Home Team KPI Audit &mdash; Generated {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}<br>
        Questions? Reply to this email.
    </p>
    </body></html>
    """

    return html


def build_manager_email(manager_data, period_label):
    """Build Joe's Monday morning motivational coaching email.

    Written to inspire action, give clarity on who to focus on, and
    make the sales manager feel like they have a clear game plan.
    """
    cs = manager_data["coaching_summary"]
    at = manager_data["agent_trends"]
    kpi = manager_data["kpi"]
    tw = manager_data["team_weeks"]
    curr_totals = tw[0]["totals"] if tw else {}

    meeting = cs["meeting_kpi"]
    total = cs["total_agents"]
    pct = cs["pct_meeting"]

    # Pick motivational opener based on team performance
    if pct >= 80:
        opener = "The team is firing on all cylinders. Let's keep the momentum going and push for 100%."
        emoji = "🔥"
    elif pct >= 50:
        opener = f"We're making progress — {meeting} out of {total} agents are hitting KPIs. A few targeted conversations this week can move the needle."
        emoji = "💪"
    elif pct >= 25:
        opener = f"Only {meeting} of {total} agents met KPIs last week. This is your opportunity to coach the team up. Small wins compound."
        emoji = "📈"
    else:
        opener = f"Tough week — only {meeting} of {total} hit KPIs. But every great team has weeks like this. Your coaching this week will set the tone for the rest of the month."
        emoji = "🎯"

    html = f"""
    <html>
    <head><style>
        body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; max-width: 680px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1 {{ color: #1a1a2e; font-size: 20px; margin-bottom: 4px; }}
        h2 {{ color: #0f3460; font-size: 16px; margin-top: 28px; margin-bottom: 8px; border-bottom: 2px solid #e8e8e8; padding-bottom: 4px; }}
        .opener {{ background: linear-gradient(135deg, #0f3460, #16213e); color: white; padding: 20px 24px; border-radius: 10px; margin: 16px 0; font-size: 15px; line-height: 1.7; }}
        table {{ border-collapse: collapse; width: 100%; margin: 8px 0 16px; font-size: 13px; }}
        th {{ background: #0f3460; color: white; padding: 8px 10px; text-align: left; font-size: 12px; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #e8e8e8; }}
        tr:nth-child(even) {{ background: #f8f9fb; }}
        .grade {{ font-size: 16px; font-weight: 800; text-align: center; }}
        .grade-A {{ color: #28a745; }} .grade-B {{ color: #3B8AFF; }} .grade-C {{ color: #e67e22; }}
        .grade-D {{ color: #dc3545; }} .grade-F {{ color: #dc3545; }}
        .pass {{ color: #28a745; font-weight: 600; }}
        .fail {{ color: #dc3545; font-weight: 600; }}
        .muted {{ color: #999; }}
        .action {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 14px 18px; margin: 10px 0; border-radius: 6px; }}
        .action.red {{ background: #f8d7da; border-left-color: #dc3545; }}
        .action.green {{ background: #d4edda; border-left-color: #28a745; }}
        .action.blue {{ background: #e8f0fe; border-left-color: #3B8AFF; }}
        .stat-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }}
        .stat-box {{ background: #f0f4f8; border-radius: 8px; padding: 14px 16px; flex: 1; min-width: 90px; text-align: center; }}
        .stat-box .num {{ font-size: 26px; font-weight: 700; color: #0f3460; }}
        .stat-box .lbl {{ font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
        .tag {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
        .tag-pass {{ background: #d4edda; color: #155724; }}
        .tag-fail {{ background: #f8d7da; color: #721c24; }}
        .tag-warn {{ background: #fff3cd; color: #856404; }}
        .footer {{ color: #888; font-size: 11px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; }}
    </style></head>
    <body>

    <div style="background:#0f172a;border-radius:10px;padding:20px 24px;margin-bottom:16px;text-align:center">
      <img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="140" style="display:block;margin:0 auto 10px;width:140px;height:auto">
      <h1 style="margin:0;color:#ffffff;font-size:18px">{emoji} Monday Game Plan — {period_label}</h1>
      <p style="margin:4px 0 0;color:rgba(255,255,255,0.6);font-size:13px">Calls ≥{kpi['min_calls']} &bull; Convos ≥{kpi['min_convos']} &bull; OOC ≤{kpi['max_ooc']}</p>
    </div>

    <div class="opener">
        <strong>Good morning, Joe.</strong><br><br>
        {opener}<br><br>
        <strong>{meeting}/{total}</strong> agents met KPIs last week. Here's your coaching playbook.
    </div>
    """

    # ---- Team Snapshot ----
    html += """<h2>📊 Team Numbers</h2><div class="stat-row">"""
    html += f'<div class="stat-box"><div class="num">{curr_totals.get("calls",0)}</div><div class="lbl">Calls</div></div>'
    html += f'<div class="stat-box"><div class="num">{curr_totals.get("convos",0)}</div><div class="lbl">Convos</div></div>'
    html += f'<div class="stat-box"><div class="num">{curr_totals.get("texts",0)}</div><div class="lbl">Texts</div></div>'
    html += f'<div class="stat-box"><div class="num">{curr_totals.get("appts_set",0)}</div><div class="lbl">Appts</div></div>'
    html += f'<div class="stat-box"><div class="num">{cs["team_call_to_convo"]}%</div><div class="lbl">Connect Rate</div></div>'
    html += "</div>"

    # ---- Scorecard ----
    html += '<h2>📋 Agent Scorecard</h2>'
    html += '<table><tr><th>Grade</th><th>Agent</th><th>Calls</th><th>Convos</th><th>Texts</th><th>Appts</th><th>Connect %</th><th>KPI</th></tr>'
    for a in at:
        c = a["current"]
        grade_cls = f"grade-{a['grade']}"
        kpi_tag = '<span class="tag tag-pass">PASS</span>' if a["kpi_pass"] else '<span class="tag tag-fail">FAIL</span>'
        html += f"""<tr>
            <td class="grade {grade_cls}">{a['grade']}</td>
            <td><strong>{a['name']}</strong></td>
            <td>{c['calls']}</td>
            <td>{c['convos']}</td>
            <td>{c.get('texts',0)}</td>
            <td>{c['appts_set']}</td>
            <td>{a['call_to_convo']}%</td>
            <td>{kpi_tag}</td>
        </tr>"""
    html += "</table>"

    # ---- Your Coaching Plan ----
    html += '<h2>🎯 Your Coaching Plan</h2>'

    # Accountability
    acct = [a for a in at if a["coaching_type"] == "accountability"]
    if acct:
        html += '<div class="action red"><strong>🚨 Accountability 1-on-1s (Schedule ASAP)</strong><br>'
        html += '<p style="font-size:13px;margin:6px 0">These agents need a direct conversation about activity expectations. Focus on: <em>call blocking, daily schedule, removing distractions.</em></p>'
        for a in acct:
            html += f'<p style="margin:4px 0"><strong>{a["name"]}</strong> (Grade {a["grade"]}) — {a["insights"][0] if a["insights"] else "Needs discussion"}</p>'
        html += '</div>'

    # Skill coaching
    skill = [a for a in at if a["coaching_type"] == "skill"]
    if skill:
        html += '<div class="action"><strong>📋 Skill Coaching (Role-play / Call Review)</strong><br>'
        html += '<p style="font-size:13px;margin:6px 0">These agents are making effort but need help converting. Try: <em>listen to their last 3 calls, role-play objection handling, review scripts.</em></p>'
        for a in skill:
            html += f'<p style="margin:4px 0"><strong>{a["name"]}</strong> (Grade {a["grade"]}) — {a["insights"][0] if a["insights"] else "Conversion coaching"}</p>'
        html += '</div>'

    # Praise
    praise = [a for a in at if a["coaching_type"] == "praise"]
    if praise:
        html += '<div class="action green"><strong>🌟 Recognize This Week</strong><br>'
        html += '<p style="font-size:13px;margin:6px 0">Public praise reinforces the behavior you want. Consider: team shoutout, pair them with struggling agents as mentors.</p>'
        for a in praise:
            html += f'<p style="margin:4px 0"><strong>{a["name"]}</strong> (Grade {a["grade"]}) — {a["insights"][0] if a["insights"] else "Meeting KPIs"}</p>'
        html += '</div>'

    # ---- Quick Wins ----
    html += '<h2>⚡ Quick Wins for This Week</h2>'
    html += '<div class="action blue">'
    html += '<ol style="font-size:13px;margin:0;padding-left:20px">'

    # Dynamic quick wins based on data
    if acct:
        html += f'<li><strong>Schedule 1-on-1s with {", ".join(a["name"].split()[0] for a in acct)}</strong> — by Tuesday EOD.</li>'
    if skill:
        html += f'<li><strong>Pull call recordings for {", ".join(a["name"].split()[0] for a in skill)}</strong> — listen to their last 3 calls and prep coaching notes.</li>'

    # Text activity insight
    low_text = [a for a in at if a["current"].get("texts", 0) < 10 and a["current"]["calls"] > 0]
    if low_text:
        html += f'<li><strong>Text follow-up gap:</strong> {", ".join(a["name"].split()[0] for a in low_text)} have low text output. Remind agents: every missed call needs a text follow-up.</li>'

    # Connect rate insight
    low_connect = [a for a in at if a["call_to_convo"] < 5 and a["current"]["calls"] >= 10]
    if low_connect:
        html += f'<li><strong>Timing review:</strong> {", ".join(a["name"].split()[0] for a in low_connect)} have very low connect rates. Check what times they\'re calling — early morning and 4-6pm convert best.</li>'

    if praise:
        html += f'<li><strong>Team huddle shoutout:</strong> Recognize {", ".join(a["name"].split()[0] for a in praise)} in front of the team.</li>'

    html += '<li><strong>End of week:</strong> Check if KPI numbers improved before the next Monday audit.</li>'
    html += '</ol></div>'

    # Footer
    html += f"""
    <p class="footer">
        Legacy Home Team KPI Audit — Generated {datetime.now().strftime('%A, %B %d at %I:%M %p')}<br>
        View the full dashboard: <a href="https://web-production-80a1e.up.railway.app/">KPI Dashboard</a>
    </p>
    </body></html>
    """
    return html


def send_manager_email(manager_data, period_label):
    """Send Joe's Monday morning coaching email. CC Barry."""
    html_body = build_manager_email(manager_data, period_label)
    cs = manager_data["coaching_summary"]
    meeting = cs["meeting_kpi"]
    total = cs["total_agents"]

    subject = _catchy_subject("manager", {"meeting": meeting, "total": total})

    # Joe + Barry — deduplicated
    recipients = [getattr(config, "MANAGER_EMAIL", "thejoefu@gmail.com")] + list(config.EMAIL_RECIPIENTS)
    seen = set()
    unique = [e for e in recipients if e not in seen and not seen.add(e)]

    try:
        _pm.send(
            to=", ".join(unique),
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html_body,
        )
        print(f"\n✅ Manager email sent to {len(unique)} recipients")
        return True
    except Exception as e:
        print(f"\n❌ Failed to send manager email: {e}")
        return False


LIVE_TRANSFER_NUMBER = "(757) 960-1491"
LIVE_TRANSFER_NUMBER_RAW = "7579601491"


def build_hype_email(agents, period_label, ai_text_count, ai_voice_count,
                     fhalen_appts, fhalen_name="Fhalen", thresholds=None):
    """
    Build the weekly KPI hype email.

    agents        — list of agent dicts from run_audit_data() (all agents, pass + fail)
    period_label  — e.g. "Apr 14 – Apr 19, 2026"
    ai_text_count — leads with AI_NEEDS_FOLLOW_UP tag last week
    ai_voice_count— leads with AI_VOICE_NEEDS_FOLLOW_UP tag last week
    fhalen_appts  — ISA appointments set last week (int)
    thresholds    — dict with min_calls, min_convos, max_ooc (from audit data)

    Returns HTML string.
    """
    from datetime import datetime

    passing  = [a for a in agents if a["evaluation"]["overall_pass"]]
    n_agents = max(len(agents), 1)

    # ── KPI thresholds (for congrats copy — read from saved settings) ────────
    thresh      = thresholds or {}
    min_calls   = thresh.get("min_calls",  30)
    min_convos  = thresh.get("min_convos",  5)
    max_ooc     = thresh.get("max_ooc",    50)

    # ── Team averages (all agents — honest comparison base) ─────────────────
    total_calls  = sum(a["metrics"]["outbound_calls"] for a in agents)
    total_convos = sum(a["metrics"]["conversations"] for a in agents)
    avg_calls  = total_calls  / n_agents
    avg_convos = total_convos / n_agents

    # ── Lead distribution total ─────────────────────────────────────────────
    total_leads = ai_text_count + ai_voice_count + fhalen_appts

    # ── Build per-agent performance narrative ────────────────────────────────
    def agent_narrative(a):
        calls  = a["metrics"]["outbound_calls"]
        convos = a["metrics"]["conversations"]
        first  = a["name"].split()[0]
        calls_pct  = round((calls  - avg_calls)  / max(avg_calls,  1) * 100)
        convos_pct = round((convos - avg_convos) / max(avg_convos, 1) * 100)

        parts = []
        if convos_pct > 0:
            parts.append(f"<strong>{convos_pct}% more conversations</strong> than the team average")
        if calls_pct > 0:
            parts.append(f"<strong>{calls_pct}% more dials</strong> than the team average")

        if len(parts) == 2:
            stat_line = f"{parts[0]} and {parts[1]}"
        elif len(parts) == 1:
            stat_line = parts[0]
        else:
            stat_line = "hit their call and conversation targets this week"

        return first, stat_line, convos, calls, convos_pct, calls_pct

    now_str = datetime.now().strftime("%B %d, %Y")
    week_of = datetime.now().strftime("%b %d")

    html = f"""
<html><head><meta charset="utf-8"><style>
body{{font-family:-apple-system,'Segoe UI',Arial,sans-serif;color:#1a1a2e;max-width:680px;margin:0 auto;padding:16px;background:#f5f7fa}}
</style></head><body>
<div style="max-width:680px;margin:0 auto;font-family:-apple-system,'Segoe UI',Arial,sans-serif">

<!-- HEADER -->
<div style="background:#0f172a;border-radius:12px;padding:24px;margin-bottom:20px;text-align:center">
  <img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="130" style="display:block;margin:0 auto 12px;width:130px;height:auto">
  <p style="margin:0;color:#f59e0b;font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:2px">Live Transfer Weekly</p>
  <h1 style="margin:6px 0 4px;color:#ffffff;font-size:24px;font-weight:800">Who Earned the Board This Week</h1>
  <p style="margin:0;color:rgba(255,255,255,0.5);font-size:13px">Week of {week_of} &nbsp;·&nbsp; {period_label}</p>
</div>

<!-- LEAD DISTRIBUTION STAT -->
"""
    if total_leads > 0:
        detail_parts = []
        if ai_text_count > 0:
            detail_parts.append(f"{ai_text_count} AI text")
        if ai_voice_count > 0:
            detail_parts.append(f"{ai_voice_count} AI voice")
        if fhalen_appts > 0:
            detail_parts.append(f"{fhalen_appts} Human ISA appointments")
        detail_str = " + ".join(detail_parts)

        html += f"""
<div style="background:#fff;border-radius:10px;border:2px solid #f59e0b;padding:20px 24px;margin-bottom:20px;text-align:center">
  <p style="margin:0 0 4px;font-size:13px;text-transform:uppercase;letter-spacing:1px;color:#6b7280">Leads Distributed Last Week</p>
  <div style="font-size:52px;font-weight:900;color:#0f172a;line-height:1">{total_leads}</div>
  <p style="margin:6px 0 0;font-size:14px;color:#6b7280">{detail_str}</p>
  <p style="margin:10px 0 0;font-size:14px;color:#374151;line-height:1.5">
    {total_leads} real people reached out, engaged with our AI, or sat down with our ISA
    — and are waiting for an agent to pick up where the tech left off.
    The only question is: <strong>whose phone does it ring?</strong>
  </p>
</div>
"""
    else:
        html += f"""
<div style="background:#fff;border-radius:10px;border:1px solid #e5e7eb;padding:20px 24px;margin-bottom:20px;text-align:center">
  <p style="margin:0;font-size:14px;color:#6b7280">Lead distribution data not yet available for this week.</p>
</div>
"""

    # ── QUALIFYING AGENTS ────────────────────────────────────────────────────
    n_passing = len(passing)
    if n_passing > 0:
        # Build first-name list for the congrats line
        pass_first = [a["name"].split()[0] for a in passing]
        if len(pass_first) > 1:
            pass_names = ", ".join(pass_first[:-1]) + " and " + pass_first[-1]
        else:
            pass_names = pass_first[0]
        agent_word = "agent has" if n_passing == 1 else f"{n_passing} agents have"

        html += f"""
<div style="background:#0f172a;border-radius:10px;padding:20px 24px;margin-bottom:12px">
  <p style="margin:0 0 6px;color:#f59e0b;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:2px">🏆 This Week's Live Transfer Roster</p>
  <h2 style="margin:0 0 10px;color:#ffffff;font-size:20px;font-weight:900;line-height:1.2">
    Congrats, {pass_names}!
  </h2>
  <p style="margin:0;color:rgba(255,255,255,0.8);font-size:14px;line-height:1.6">
    {agent_word} earned direct routing of ISA appointments and AI live transfers
    this week by hitting our KPI standards:
    <strong style="color:#f59e0b">{min_calls} outbound calls</strong>,
    <strong style="color:#f59e0b">{min_convos} conversations</strong>, and
    <strong style="color:#f59e0b">fewer than {max_ooc} out-of-compliance leads</strong>.
    That's the bar. You cleared it.
  </p>
</div>
"""
        html += '<div style="display:flex;flex-direction:column;gap:10px;margin-bottom:20px">'
        for a in passing:
            first, stat_line, convos, calls, convos_pct, calls_pct = agent_narrative(a)
            html += f"""
<div style="background:#fff;border-radius:10px;border-left:4px solid #f59e0b;padding:16px 20px;display:flex;align-items:flex-start;gap:12px">
  <div style="background:#f59e0b;color:#0f172a;font-weight:900;font-size:18px;width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;flex-shrink:0;text-align:center;line-height:40px">
    {first[0].upper()}
  </div>
  <div>
    <div style="font-size:17px;font-weight:800;color:#0f172a;margin-bottom:4px">{a['name']}</div>
    <div style="font-size:14px;color:#374151;line-height:1.5">
      {a['name'].split()[0]} {stat_line}.
    </div>
  </div>
</div>
"""
        html += "</div>"
    else:
        html += f"""
<div style="background:#0f172a;border-radius:10px;padding:20px 24px;margin-bottom:12px">
  <p style="margin:0 0 6px;color:#f59e0b;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:2px">🏆 This Week's Live Transfer Roster</p>
  <h2 style="margin:0 0 10px;color:#ffffff;font-size:20px;font-weight:900">Nobody qualified this week.</h2>
  <p style="margin:0;color:rgba(255,255,255,0.7);font-size:14px;line-height:1.6">
    The standard is {min_calls} outbound calls, {min_convos} conversations,
    and fewer than {max_ooc} out-of-compliance leads. The line resets Monday — it's wide open.
  </p>
</div>
"""

    # ── THE PRIZE ────────────────────────────────────────────────────────────
    if passing:
        html += f"""
<div style="background:#fff;border-radius:10px;border:1px solid #e5e7eb;padding:20px 24px;margin-bottom:20px">
  <h2 style="margin:0 0 14px;color:#0f172a;font-size:16px;font-weight:800">What You Earned</h2>

  <div style="display:flex;flex-direction:column;gap:10px">

    <div style="display:flex;align-items:flex-start;gap:12px;padding:12px;background:#f0fdf4;border-radius:8px;border:1px solid #bbf7d0">
      <span style="font-size:22px">💬</span>
      <div>
        <div style="font-weight:700;color:#15803d;font-size:14px">AI Text Live Transfers</div>
        <div style="font-size:13px;color:#374151;margin-top:2px">
          When a lead responds to our AI texting campaign and is ready to talk, you're first in line.
          That transfer goes to you — not to whoever happens to be logged in.
        </div>
      </div>
    </div>

    <div style="display:flex;align-items:flex-start;gap:12px;padding:12px;background:#eff6ff;border-radius:8px;border:1px solid #bfdbfe">
      <span style="font-size:22px">🤖</span>
      <div>
        <div style="font-weight:700;color:#1d4ed8;font-size:14px">AI Voice Live Transfers</div>
        <div style="font-size:13px;color:#374151;margin-top:2px">
          When our AI voice system qualifies a caller and they want to speak to a real agent,
          you're the next ring. AI did the work. You close it.
        </div>
      </div>
    </div>

    <div style="display:flex;align-items:flex-start;gap:12px;padding:12px;background:#fefce8;border-radius:8px;border:1px solid #fde68a">
      <span style="font-size:22px">📧</span>
      <div>
        <div style="font-weight:700;color:#b45309;font-size:14px">AI Email Live Transfers</div>
        <div style="font-size:13px;color:#374151;margin-top:2px">
          Leads that engage with our automated email sequences get routed to qualifying agents
          — not the general pool. You're in the priority queue.
        </div>
      </div>
    </div>

  </div>
</div>

<!-- BIG PHONE NUMBER -->
<div style="background:#0f172a;border-radius:12px;padding:28px 24px;margin-bottom:20px;text-align:center">
  <p style="margin:0 0 4px;color:rgba(255,255,255,0.5);font-size:12px;text-transform:uppercase;letter-spacing:2px">Your Live Transfer Number</p>
  <div style="font-size:38px;font-weight:900;color:#f59e0b;letter-spacing:2px;margin:8px 0">{LIVE_TRANSFER_NUMBER}</div>
  <p style="margin:0 0 16px;color:rgba(255,255,255,0.7);font-size:14px;line-height:1.5">
    When this number rings, it's a warm, AI-qualified lead ready to talk to an agent.
    Not a cold call. Not a nurture. <strong style="color:#ffffff">Someone who wants to buy or sell.</strong>
  </p>
  <a href="tel:{LIVE_TRANSFER_NUMBER_RAW}"
     style="display:inline-block;background:#f59e0b;color:#0f172a;font-weight:800;font-size:15px;
            padding:12px 28px;border-radius:8px;text-decoration:none;letter-spacing:0.5px">
    📱 Save This Number Now
  </a>
</div>
"""

    # ── FOR THE REST OF THE TEAM ─────────────────────────────────────────────
    failing = [a for a in agents if not a["evaluation"]["overall_pass"]]
    if failing:
        fail_first = [a["name"].split()[0] for a in failing]
        if len(fail_first) > 1:
            fail_names = ", ".join(fail_first[:-1]) + " and " + fail_first[-1]
        else:
            fail_names = fail_first[0]
        n_fail = len(failing)

        html += f"""
<div style="background:#fff;border-radius:10px;border:1px solid #e5e7eb;padding:20px 24px;margin-bottom:20px">
  <h2 style="margin:0 0 10px;color:#0f172a;font-size:15px;font-weight:700">The Board Resets Monday</h2>
  <p style="margin:0 0 10px;font-size:14px;color:#374151;line-height:1.6">
    {fail_names} — your name could be on this email next week.
    Every single one of those {total_leads} leads from last week? There are more coming this week.
    The system doesn't sleep. The only question is whether you're on the list when they do.
  </p>
  <p style="margin:0;font-size:14px;color:#374151;line-height:1.6">
    Make your calls. Have your conversations. The phone line doesn't care how you feel on a Tuesday morning.
    Neither does the leaderboard. But it does reward the people who show up anyway.
  </p>
  <p style="margin:10px 0 0;font-size:14px;font-style:italic;color:#6b7280">
    "You're being too nice to your comfort zone." — Barry
  </p>
</div>
"""

    # ── FOOTER ───────────────────────────────────────────────────────────────
    html += f"""
<div style="text-align:center;color:#9ca3af;font-size:11px;margin-top:16px;padding-top:12px;border-top:1px solid #e5e7eb">
  Legacy Home Team &nbsp;·&nbsp; Generated {now_str}<br>
  Questions? Reply to this email or text Barry directly.
</div>

</div>
</body></html>
"""
    return html


def send_hype_email(agents, period_label, ai_text_count, ai_voice_count,
                    fhalen_appts, fhalen_name, to_emails, thresholds=None):
    """
    Send the weekly KPI hype email to all team members.
    to_emails: list of (name, email) tuples from get_all_user_emails()
    Returns (success: bool, message: str)
    """
    html_body = build_hype_email(
        agents, period_label, ai_text_count, ai_voice_count,
        fhalen_appts, fhalen_name, thresholds=thresholds,
    )

    passing_count = sum(1 for a in agents if a["evaluation"]["overall_pass"])
    total_count   = len(agents)
    week_of       = datetime.now().strftime("%b %d")

    subjects = [
        f"🔥 {passing_count}/{total_count} Agents Earned the Live Transfer Line — Save (757) 960-1491",
        f"📞 Who's Getting Live Transfers This Week — {week_of}",
        f"🏆 Legacy Live Board — {passing_count} Agents Qualified — Here's What That Means",
        f"⚡ Live Transfers, AI Leads & the Phone Number You Need to Save — {week_of}",
    ]
    import random as _random
    subject = _random.choice(subjects)

    valid = [(name, email) for name, email in to_emails if email]
    if not valid:
        return False, "No valid email recipients found"

    try:
        _pm.send(
            to=", ".join(f"{name} <{email}>" for name, email in valid),
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html_body,
        )
        return True, f"Sent to {len(valid)} recipients"
    except Exception as e:
        return False, str(e)


def send_report(results, period_start, period_end):
    """Send the audit report via Postmark."""
    if not config.EMAIL_RECIPIENTS:
        print("\n⚠  No EMAIL_RECIPIENTS configured. Skipping email.")
        return False

    html_body = build_html_report(results, period_start, period_end)

    passed = sum(1 for d in results.values() if d["evaluation"]["overall_pass"])
    total = len(results)
    subject = _catchy_subject("audit", {"passed": passed, "total": total})

    # Add Fhalen (Live Calls admin) to recipients if not already included
    all_recipients = list(config.EMAIL_RECIPIENTS)
    admin_email = getattr(config, "LIVE_CALLS_ADMIN_EMAIL", None)
    if admin_email and admin_email not in all_recipients:
        all_recipients.append(admin_email)

    try:
        _pm.send(
            to=", ".join(all_recipients),
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html_body,
        )
        print(f"\n✅ Weekly update emailed to {len(all_recipients)} recipients:")
        for email in all_recipients:
            label = " (Live Calls admin)" if email == admin_email else ""
            print(f"   → {email}{label}")
        return True
    except Exception as e:
        print(f"\n❌ Failed to send email: {e}")
        return False


def build_isa_email(isa_data):
    """Build Fhalen's Monday morning ISA performance email."""
    c = isa_data.get("current", {})
    p = isa_data.get("previous", {})
    funnel = isa_data.get("funnel", {})
    handoffs = isa_data.get("handoffs", {})
    stale = isa_data.get("stale_leads", [])
    insights = isa_data.get("insights", [])
    own = isa_data.get("own_pipeline", {})
    period = isa_data.get("period", {}).get("current", "This Week")

    calls = c.get("calls", 0)
    convos = c.get("convos", 0)
    appts_set = c.get("appts_set", 0)
    appts_met = c.get("appts_met", 0)
    texts_out = c.get("texts_out", 0)
    connect_rate = c.get("connect_rate", 0)
    show_rate = c.get("show_rate", 0)

    # Motivational opener
    if convos >= 10 and appts_set >= 3:
        opener = f"Strong week — {convos} conversations and {appts_set} appointments. Keep pushing toward consistency."
        emoji = "🔥"
    elif convos >= 5:
        opener = f"Good volume with {calls} calls and {convos} conversations. Now let's focus on converting those conversations into appointments."
        emoji = "💪"
    elif calls > 100:
        opener = f"You put in the effort with {calls} calls, but only {convos} became conversations. Let's figure out what's blocking the connections."
        emoji = "📈"
    else:
        opener = f"We need to see more activity. {calls} calls and {convos} conversations isn't enough to build a healthy pipeline. Let's create a plan."
        emoji = "🎯"

    html = f"""
    <html>
    <head><style>
        body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; max-width: 680px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1 {{ color: #1a1a2e; font-size: 20px; margin-bottom: 4px; }}
        h2 {{ color: #0f3460; font-size: 16px; margin-top: 28px; margin-bottom: 8px; border-bottom: 2px solid #e8e8e8; padding-bottom: 4px; }}
        .opener {{ background: linear-gradient(135deg, #0f3460, #16213e); color: white; padding: 20px 24px; border-radius: 10px; margin: 16px 0; font-size: 15px; line-height: 1.7; }}
        .stat-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }}
        .stat-box {{ background: #f0f4f8; border-radius: 8px; padding: 14px 16px; flex: 1; min-width: 90px; text-align: center; }}
        .stat-box .num {{ font-size: 26px; font-weight: 700; color: #0f3460; }}
        .stat-box .lbl {{ font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 8px 0 16px; font-size: 13px; }}
        th {{ background: #0f3460; color: white; padding: 8px 10px; text-align: left; font-size: 12px; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #e8e8e8; }}
        tr:nth-child(even) {{ background: #f8f9fb; }}
        .pass {{ color: #28a745; font-weight: 600; }}
        .fail {{ color: #dc3545; font-weight: 600; }}
        .muted {{ color: #999; }}
        .action {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 14px 18px; margin: 10px 0; border-radius: 6px; font-size: 13px; }}
        .action.red {{ background: #f8d7da; border-left-color: #dc3545; }}
        .action.green {{ background: #d4edda; border-left-color: #28a745; }}
        .funnel {{ display: flex; align-items: center; gap: 4px; margin: 12px 0; font-size: 14px; }}
        .funnel .step {{ text-align: center; padding: 10px; border-radius: 8px; flex: 1; }}
        .funnel .arrow {{ color: #ccc; font-size: 20px; }}
        .footer {{ color: #888; font-size: 11px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; }}
    </style></head>
    <body>

    <div style="background:#0f172a;border-radius:10px;padding:20px 24px;margin-bottom:16px;text-align:center">
      <img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="140" style="display:block;margin:0 auto 10px;width:140px;height:auto">
      <h1 style="margin:0;color:#ffffff;font-size:18px">{emoji} ISA Weekly Performance — {period}</h1>
    </div>

    <div class="opener">
        <strong>Fhalen,</strong><br><br>
        {opener}
    </div>
    """

    # ── Conversion Funnel ──
    html += '<h2>📞 Your Conversion Funnel</h2>'
    html += '<div class="funnel">'
    html += f'<div class="step" style="background:#e8f0fe"><div style="font-size:24px;font-weight:700">{funnel.get("dialed",0)}</div><div style="font-size:10px;color:#666">DIALED</div></div>'
    html += '<div class="arrow">→</div>'
    html += f'<div class="step" style="background:#e8f0fe"><div style="font-size:24px;font-weight:700">{funnel.get("connected",0)}</div><div style="font-size:10px;color:#666">CONNECTED</div></div>'
    html += '<div class="arrow">→</div>'
    html += f'<div class="step" style="background:#d4edda"><div style="font-size:24px;font-weight:700">{convos}</div><div style="font-size:10px;color:#666">CONVERSATIONS</div></div>'
    html += '<div class="arrow">→</div>'
    html += f'<div class="step" style="background:#fff3cd"><div style="font-size:24px;font-weight:700">{appts_set}</div><div style="font-size:10px;color:#666">APPTS SET</div></div>'
    html += '<div class="arrow">→</div>'
    html += f'<div class="step" style="background:{"#d4edda" if appts_met > 0 else "#f8d7da"}"><div style="font-size:24px;font-weight:700">{appts_met}</div><div style="font-size:10px;color:#666">APPTS MET</div></div>'
    html += '</div>'

    # ── Key Numbers ──
    html += '<h2>📊 Key Numbers</h2><div class="stat-row">'
    html += f'<div class="stat-box"><div class="num">{calls}</div><div class="lbl">Calls</div></div>'
    html += f'<div class="stat-box"><div class="num">{texts_out}</div><div class="lbl">Texts Sent</div></div>'
    html += f'<div class="stat-box"><div class="num">{connect_rate}%</div><div class="lbl">Connect Rate</div></div>'
    html += f'<div class="stat-box"><div class="num">{show_rate}%</div><div class="lbl">Show Rate</div></div>'
    html += '</div>'

    # ── Insights ──
    if insights:
        html += '<h2>💡 Coaching Insights</h2>'
        for ins in insights:
            box_cls = "red" if ins["type"] == "critical" else ("" if ins["type"] == "warning" else "green")
            html += f'<div class="action {box_cls}"><strong>{ins["icon"]} {ins["title"]}</strong><br>{ins["detail"]}</div>'

    # ── Pipeline Health ──
    if own and own.get("total", 0) > 0:
        html += f'<h2>📂 Your Pipeline — {own["total"]} Leads</h2>'
        stuck = own.get("stuck_in_lead", 0)
        stale_14d = own.get("stale_14d", 0)
        if stuck > 0:
            html += f'<div class="action red"><strong>{stuck} leads are stuck in "Lead" stage.</strong> These need to be qualified and either handed off to an agent or moved to nurture. Every day they sit idle, they cool off.</div>'
        if stale_14d > 0:
            html += f'<div class="action red"><strong>{stale_14d} leads have had no activity in 14+ days.</strong> Re-engage or move to long-term drip.</div>'

    # ── Stale Handoffs ──
    if stale:
        html += f'<h2>🔴 {len(stale)} Leads Need Your Follow-Up</h2>'
        html += '<p style="font-size:13px;color:#666">You connected with these leads and handed them off, but they\'ve gone cold. Circle back — they were warm when you found them.</p>'
        html += '<table><tr><th>Lead</th><th>Agent</th><th>Stage</th><th>Days Stale</th></tr>'
        for sl in stale[:15]:
            html += f'<tr><td>{sl["name"]}</td><td>{sl["assigned_to"]}</td><td>{sl["stage"]}</td><td class="fail">{sl["days_stale"]}d</td></tr>'
        html += '</table>'
        if len(stale) > 15:
            html += f'<p class="muted">+ {len(stale) - 15} more — see dashboard for full list</p>'

    # ── Footer ──
    html += f"""
    <p class="footer">
        Legacy Home Team ISA Report — Generated {datetime.now().strftime('%A, %B %d at %I:%M %p')}<br>
        View the full dashboard: <a href="https://web-production-80a1e.up.railway.app/">KPI Dashboard</a>
    </p>
    </body></html>
    """
    return html


def send_isa_email(isa_data):
    """Send Fhalen's Monday morning ISA performance email."""
    html_body = build_isa_email(isa_data)
    c = isa_data.get("current", {})
    subject = _catchy_subject("isa", {"calls": c.get("calls", 0), "convos": c.get("convos", 0)})

    recipients = list(config.EMAIL_RECIPIENTS)
    admin_email = getattr(config, "LIVE_CALLS_ADMIN_EMAIL", None)
    if admin_email and admin_email not in recipients:
        recipients.append(admin_email)

    seen = set()
    unique = [e for e in recipients if e not in seen and not seen.add(e)]

    try:
        _pm.send(
            to=", ".join(unique),
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html_body,
        )
        print(f"\n✅ ISA email sent to {len(unique)} recipients")
        return True
    except Exception as e:
        print(f"\n❌ Failed to send ISA email: {e}")
        return False


# ---- Appointment Accountability Email ----


FUB_PERSON_URL = "https://yourfriendlyagent.followupboss.com/2/people/view/{person_id}"

LOGO_WHITE_URL = "https://web-production-3363cc.up.railway.app/static/logo-white.png"
LOGO_HEADER_IMG = f'<img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="140" style="display:block;margin:0 auto 10px;width:140px;height:auto">'

# Inline style constants — using inline styles on every element so formatting
# survives forwarding (email clients strip <style> blocks on forward).
_S = {
    "body":        "font-family:-apple-system,'Segoe UI',Roboto,Arial,sans-serif;color:#1e293b;max-width:640px;margin:0 auto;background:#f1f5f9;padding:16px",
    "header":      "background:#0f172a;color:white;padding:20px 24px;border-radius:10px;margin-bottom:12px",
    "header_h1":   "margin:0;font-size:18px;font-weight:700;color:white",
    "header_p":    "margin:4px 0 0;font-size:13px;color:rgba(255,255,255,0.65)",
    "sum_table":   "width:100%;border-collapse:collapse;margin-bottom:16px",
    "sum_td":      "background:white;border-radius:8px;padding:10px 14px;text-align:center;border:1px solid #e2e8f0",
    "sum_label":   "font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;display:block;margin-top:2px",
    "card":        "background:white;border-radius:10px;padding:20px;margin-bottom:12px;border:1px solid #e2e8f0",
    "card_h2":     "margin:0 0 4px;font-size:15px;font-weight:700;color:#0f172a",
    "card_sub":    "font-size:12px;color:#64748b;margin:0 0 14px",
    "chip_stale":  "display:inline-block;padding:5px 10px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none;white-space:nowrap;background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;margin:3px",
    "chip_over":   "display:inline-block;padding:5px 10px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none;white-space:nowrap;background:#fffbeb;color:#b45309;border:1px solid #fde68a;margin:3px",
    "chip_pend":   "display:inline-block;padding:5px 10px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none;white-space:nowrap;background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;margin:3px",
    "legend":      "font-size:11px;color:#94a3b8;margin-top:10px",
    "dot_red":     "display:inline-block;width:8px;height:8px;border-radius:50%;background:#fca5a5;margin-right:3px;vertical-align:middle",
    "dot_amber":   "display:inline-block;width:8px;height:8px;border-radius:50%;background:#fcd34d;margin-right:3px;vertical-align:middle",
    "dot_blue":    "display:inline-block;width:8px;height:8px;border-radius:50%;background:#93c5fd;margin-right:3px;vertical-align:middle",
    "btn":         "display:inline-block;background:#3b82f6;color:white;padding:10px 22px;border-radius:7px;text-decoration:none;font-size:13px;font-weight:600",
    "footer":      "text-align:center;font-size:11px;color:#94a3b8;margin-top:20px;padding:12px",
}


def build_appointment_email(appt_data):
    """Build the appointment accountability email — all styles inlined so
    formatting survives email client forwarding."""
    t = appt_data.get("totals", {})
    agents = appt_data.get("agents", [])
    appts = appt_data.get("appointments", [])
    period = appt_data.get("period", "")

    pct = t.get("completion_rate", 0)
    pct_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 40 else "#ef4444"

    from collections import defaultdict
    agent_open = defaultdict(list)
    for a in appts:
        if a.get("is_past") and not a.get("outcome") and a.get("tier"):
            agent_open[a.get("assigned_agent", "Unknown")].append(a)

    # ── Header ──────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="{_S['body']}">

<div style="{_S['header']};text-align:center">
  {LOGO_HEADER_IMG}
  <h1 style="{_S['header_h1']};text-align:center">&#128197; Appointment Accountability &mdash; {period}</h1>
  <p style="{_S['header_p']};text-align:center">Weekly outcome review &middot; Legacy Home Team</p>
</div>
"""

    # ── Manager summary strip (table for email client compat) ───────────────
    stat_cells = [
        (str(t.get("total_30d", 0)), "#3b82f6", "Total"),
        (f"{pct}%",                   pct_color,  "Complete"),
        (str(t.get("met", 0)),        "#22c55e",  "Met"),
        (str(t.get("no_show", 0)),    "#f59e0b",  "No Show"),
        (str(t.get("no_outcome", 0)), "#ef4444",  "Open"),
        (str(t.get("stale_7d", 0)),   "#ef4444",  "7d+ Stale"),
    ]
    html += f'<table style="{_S["sum_table"]}"><tr>'
    for val, color, label in stat_cells:
        html += (f'<td style="{_S["sum_td"]}">'
                 f'<span style="font-size:22px;font-weight:700;color:{color}">{val}</span>'
                 f'<span style="{_S["sum_label"]}">{label}</span></td>')
    html += "</tr></table>\n"

    # ── Per-agent cards ──────────────────────────────────────────────────────
    sorted_agents = sorted(agents, key=lambda a: a.get("no_outcome", 0), reverse=True)
    tier_order = {"stale": 0, "overdue": 1, "pending": 2, "recent": 3}

    for ag in sorted_agents:
        name = ag.get("name", "Unknown")
        open_list = agent_open.get(name, [])
        if not open_list:
            continue

        no_outcome = ag.get("no_outcome", 0)
        stale_ct   = ag.get("stale", 0)
        met        = ag.get("met", 0)

        parts = []
        if no_outcome == 1:
            parts.append("1 appointment needs an outcome")
        elif no_outcome > 1:
            parts.append(f"{no_outcome} appointments need outcomes")
        if stale_ct == 1:
            parts.append("1 has gone stale (7+ days with no update)")
        elif stale_ct > 1:
            parts.append(f"{stale_ct} have gone stale (7+ days with no update)")
        summary_line = " &mdash; ".join(parts) if parts else "All caught up."

        html += f'<div style="{_S["card"]}">\n'
        html += f'  <h2 style="{_S["card_h2"]}">{name}</h2>\n'
        html += f'  <p style="{_S["card_sub"]}">{summary_line} &nbsp;&middot;&nbsp; {met} met this period</p>\n'
        html += '  <div style="line-height:2">\n'

        open_sorted = sorted(open_list, key=lambda x: tier_order.get(x.get("tier", ""), 9))
        for lead in open_sorted:
            pid       = lead.get("person_id")
            lead_name = lead.get("lead_name", "Unknown")
            tier      = lead.get("tier", "pending")
            days      = round(lead.get("days_since", 0))
            days_str  = f"{days}d ago" if days > 0 else "today"
            chip_style = _S["chip_stale"] if tier == "stale" else _S["chip_over"] if tier == "overdue" else _S["chip_pend"]

            if pid:
                fub_url = FUB_PERSON_URL.format(person_id=pid)
                html += f'    <a href="{fub_url}" style="{chip_style}" title="{days_str}">{lead_name}</a>\n'
            else:
                html += f'    <span style="{chip_style}" title="{days_str}">{lead_name}</span>\n'

        html += '  </div>\n'
        html += (f'  <p style="{_S["legend"]}">'
                 f'<span style="{_S["dot_red"]}"></span>7d+ stale&nbsp;&nbsp;'
                 f'<span style="{_S["dot_amber"]}"></span>48h+ overdue&nbsp;&nbsp;'
                 f'<span style="{_S["dot_blue"]}"></span>pending&nbsp;&nbsp;'
                 f'&middot; click any name to open in Follow Up Boss</p>\n')
        html += '</div>\n'

    if not any(agent_open.values()):
        html += f'<div style="{_S["card"]};text-align:center;color:#22c55e;font-weight:600">&#10003; All appointments are up to date!</div>\n'

    # ── Footer ───────────────────────────────────────────────────────────────
    html += f"""
<p style="text-align:center;margin:20px 0 8px">
  <a href="https://web-production-80a1e.up.railway.app/" style="{_S['btn']}">Open Dashboard</a>
</p>
<p style="{_S['footer']}">
  Legacy Home Team &middot; Appointment Accountability &middot; {datetime.now().strftime('%A, %B %d at %I:%M %p')}
</p>
</body></html>"""

    return html


def build_agent_appointment_email(appt_data, agent_name, agent_open):
    """Build an appointment email for a single agent — only their open leads,
    no other agents visible, no dashboard link."""
    t    = appt_data.get("totals", {})
    period = appt_data.get("period", "")
    pct  = t.get("completion_rate", 0)
    pct_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 40 else "#ef4444"

    # Find this agent's summary row
    ag = next((a for a in appt_data.get("agents", []) if a.get("name") == agent_name), {})
    no_outcome = ag.get("no_outcome", 0)
    stale_ct   = ag.get("stale", 0)
    met        = ag.get("met", 0)

    parts = []
    if no_outcome == 1:
        parts.append("1 appointment needs an outcome")
    elif no_outcome > 1:
        parts.append(f"{no_outcome} appointments need outcomes")
    if stale_ct == 1:
        parts.append("1 has gone stale (7+ days with no update)")
    elif stale_ct > 1:
        parts.append(f"{stale_ct} have gone stale (7+ days with no update)")
    summary_line = " &mdash; ".join(parts) if parts else "All caught up."

    tier_order = {"stale": 0, "overdue": 1, "pending": 2, "recent": 3}
    open_sorted = sorted(agent_open, key=lambda x: tier_order.get(x.get("tier", ""), 9))

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="{_S['body']}">

<div style="{_S['header']};text-align:center">
  {LOGO_HEADER_IMG}
  <h1 style="{_S['header_h1']};text-align:center">&#128197; Appointment Outcomes Needed &mdash; {period}</h1>
  <p style="{_S['header_p']};text-align:center">Legacy Home Team &middot; Please update your appointments in Follow Up Boss</p>
</div>

<div style="{_S['card']}">
  <h2 style="{_S['card_h2']}">Hi {agent_name.split()[0]},</h2>
  <p style="{_S['card_sub']}">{summary_line} &nbsp;&middot;&nbsp; {met} met this period</p>
  <div style="line-height:2">
"""
    for lead in open_sorted:
        pid       = lead.get("person_id")
        lead_name = lead.get("lead_name", "Unknown")
        tier      = lead.get("tier", "pending")
        days      = round(lead.get("days_since", 0))
        days_str  = f"{days}d ago" if days > 0 else "today"
        chip_style = _S["chip_stale"] if tier == "stale" else _S["chip_over"] if tier == "overdue" else _S["chip_pend"]
        if pid:
            fub_url = FUB_PERSON_URL.format(person_id=pid)
            html += f'    <a href="{fub_url}" style="{chip_style}" title="{days_str}">{lead_name}</a>\n'
        else:
            html += f'    <span style="{chip_style}" title="{days_str}">{lead_name}</span>\n'

    html += f"""  </div>
  <p style="{_S['legend']}">
    <span style="{_S['dot_red']}"></span>7d+ stale&nbsp;&nbsp;
    <span style="{_S['dot_amber']}"></span>48h+ overdue&nbsp;&nbsp;
    <span style="{_S['dot_blue']}"></span>pending&nbsp;&nbsp;
    &middot; click any name to open directly in Follow Up Boss
  </p>
</div>

<p style="{_S['footer']}">
  Legacy Home Team &middot; {datetime.now().strftime('%A, %B %d at %I:%M %p')}
</p>
</body></html>"""

    return html


def send_appointment_email(appt_data, subject_override=None):
    """Send per-agent appointment accountability emails via Postmark.

    Each agent with open appointments gets their own email (only their leads).
    Barry, Joe, and Fhalen are CC'd on every agent email.
    A manager summary email goes to the CC list as well.
    subject_override replaces both the agent and manager subject lines when set.
    """
    # Pull agent emails from FUB
    try:
        from fub_client import FUBClient
        client = FUBClient()
        users = client.get_users()
        agent_email_map = {u.get("name"): u.get("email") for u in users if u.get("email")}
    except Exception as e:
        print(f"\n⚠  Could not fetch agent emails from FUB: {e}")
        agent_email_map = {}

    # Group open appointments by agent
    from collections import defaultdict
    agent_open = defaultdict(list)
    for a in appt_data.get("appointments", []):
        if a.get("is_past") and not a.get("outcome") and a.get("tier"):
            agent_open[a.get("assigned_agent", "Unknown")].append(a)

    cc_emails = [e for e in config.APT_EMAIL_CC]
    sent = 0
    skipped = 0

    # ── Per-agent emails ─────────────────────────────────────────────────────
    for agent_name, open_list in agent_open.items():
        if not open_list:
            continue
        email = agent_email_map.get(agent_name)
        if not email:
            print(f"  ⚠  No email found for {agent_name}, skipping")
            skipped += 1
            continue

        # Don't email the managers/ISA as agents
        if agent_name in ("Barry Jenkins", "Joseph Fuscaldo", "Fhalen Tendencia"):
            continue

        html_body = build_agent_appointment_email(appt_data, agent_name, open_list)
        no_out = len(open_list)
        if subject_override:
            subject = subject_override
        else:
            subject = f"Action Needed: {no_out} Appointment{'s' if no_out != 1 else ''} {'Need' if no_out != 1 else 'Needs'} an Outcome"

        # CC everyone except the agent themselves
        cc = [e for e in cc_emails if e != email]

        try:
            _pm.send(
                to=email,
                from_email=config.EMAIL_FROM,
                subject=subject,
                html=html_body,
                cc=cc,
            )
            print(f"  ✅ Sent to {agent_name} <{email}> ({no_out} open)")
            sent += 1
        except Exception as e:
            print(f"  ❌ Failed for {agent_name}: {e}")

    # ── Manager summary email ────────────────────────────────────────────────
    t = appt_data.get("totals", {})
    summary_html = build_appointment_email(appt_data)
    summary_subject = subject_override or _catchy_subject("appointments", {
        "no_outcome": t.get("no_outcome", 0),
        "total": t.get("total_30d", 0),
        "completion_rate": t.get("completion_rate", 0),
    })
    mgr_recipients = list(dict.fromkeys(config.APT_EMAIL_CC))
    try:
        _pm.send(
            to=", ".join(mgr_recipients),
            from_email=config.EMAIL_FROM,
            subject=f"[Team Summary] {summary_subject}",
            html=summary_html,
        )
        print(f"\n✅ Manager summary sent to {mgr_recipients}")
    except Exception as e:
        print(f"\n❌ Manager summary failed: {e}")

    print(f"\n✅ Appointment emails: {sent} agents notified, {skipped} skipped (no email)")
    return sent > 0 or skipped == 0


# =============================================================================
# Goal Setup Onboarding Email
# Sent automatically when a new agent is detected in FUB roster sync.
# Uses identity-based framing (Cheplak / Atomic Habits) to connect emotionally.
# =============================================================================

def build_goal_onboarding_email(first_name, setup_url, dashboard_url=None):
    """Build the HTML onboarding email asking a new agent to complete goal setup."""
    _S = {
        "body": "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background: #f4f4f4;",
        "container": "max-width: 580px; margin: 32px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 12px rgba(0,0,0,0.08);",
        "header": "background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); padding: 40px 36px 32px; text-align: center;",
        "header_title": "color: #ffffff; font-size: 26px; font-weight: 700; margin: 0 0 8px; letter-spacing: -0.5px;",
        "header_sub": "color: #a0aec0; font-size: 15px; margin: 0;",
        "body_pad": "padding: 36px;",
        "greeting": "font-size: 20px; font-weight: 600; color: #1a1a2e; margin: 0 0 20px;",
        "p": "font-size: 15px; line-height: 1.7; color: #4a5568; margin: 0 0 18px;",
        "quote_box": "background: #f7fafc; border-left: 4px solid #667eea; border-radius: 0 8px 8px 0; padding: 18px 22px; margin: 24px 0;",
        "quote_text": "font-size: 16px; font-style: italic; color: #2d3748; margin: 0 0 8px; line-height: 1.6;",
        "quote_attr": "font-size: 13px; color: #718096; margin: 0;",
        "bullets": "background: #f7fafc; border-radius: 8px; padding: 20px 24px; margin: 20px 0;",
        "bullet": "font-size: 14px; color: #4a5568; line-height: 1.7; margin: 0 0 10px; padding-left: 4px;",
        "cta_wrap": "text-align: center; margin: 32px 0 24px;",
        "cta_btn": "display: inline-block; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; font-size: 16px; font-weight: 700; text-decoration: none; padding: 16px 40px; border-radius: 8px; letter-spacing: 0.3px;",
        "time_note": "text-align: center; font-size: 13px; color: #a0aec0; margin: 0 0 28px;",
        "footer": "background: #f7fafc; padding: 20px 36px; text-align: center; font-size: 12px; color: #a0aec0; border-top: 1px solid #e2e8f0;",
    }
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Welcome to Legacy Home Team</title></head>
<body style="{_S['body']}">
<div style="{_S['container']}">

  <div style="{_S['header']};text-align:center">
    <img src="{LOGO_WHITE_URL}" alt="Legacy Home Team" width="140" style="display:block;margin:0 auto 10px;width:140px;height:auto">
    <p style="{_S['header_title']}">Welcome to Legacy Home Team</p>
  </div>

  <div style="{_S['body_pad']}">
    <p style="{_S['greeting']}">Hey {first_name},</p>

    <p style="{_S['p']}">
      Welcome to the team. I mean that.
    </p>

    <p style="{_S['p']}">
      The agents who earn the most here aren't necessarily the most talented.
      They're the ones who know exactly what they're building toward and have a system
      to get there every single day. That's what we're going to build for you, starting now.
    </p>

    <p style="{_S['p']}">
      First thing: set up your goals. It takes 5 minutes and it's the foundation everything else runs on.
      Your income target, your why, your prospecting schedule. Once it's done, the system calculates
      your daily call target and tracks your pace all year long.
    </p>

    <div style="{_S['cta_wrap']}">
      <a href="{setup_url}" style="{_S['cta_btn']}">Set Up My Goals →</a>
    </div>
    <p style="{_S['time_note']}">Takes about 5 minutes &nbsp;·&nbsp; Your link is personal and secure</p>
    {f'''<p style="text-align:center;font-size:13px;color:#718096;margin:0 0 24px">
      Once you've set up your goals, bookmark your personal dashboard:<br>
      <a href="{dashboard_url}" style="color:#667eea;font-weight:600">View My Dashboard →</a>
    </p>''' if dashboard_url else ''}

    <div style="background:#eef2ff;border-radius:8px;padding:20px 24px;margin:26px 0">
      <p style="font-size:15px;font-weight:700;color:#1a1a2e;margin:0 0 8px">What happens the moment your goal is in</p>
      <p style="font-size:14px;line-height:1.7;color:#4a5568;margin:0">
        You start Fast Track, our onboarding sprint. It's a step by step path from your
        first day to your first deals: how to work your leads, what to say on the calls,
        and the daily system that makes it all add up. Setting your goal is what turns it on,
        so do that first and I'll get you rolling.
      </p>
    </div>

    <p style="{_S['p']}">
      Alongside the course, watch your inbox over the next few days. I'll walk you through
      the essentials of working here:
    </p>

    <div style="{_S['bullets']}">
      <p style="{_S['bullet']}"><strong>Day 2</strong> &nbsp; What this team is about and how we operate</p>
      <p style="{_S['bullet']}"><strong>Day 3</strong> &nbsp; Exactly how you get paid (commission splits, no surprises)</p>
      <p style="{_S['bullet']}"><strong>Day 4</strong> &nbsp; Your daily workflow in Follow Up Boss</p>
      <p style="{_S['bullet']}"><strong>Day 5</strong> &nbsp; Getting set up at LPT Realty (10 things to do this week)</p>
      <p style="{_S['bullet']}"><strong>Day 6</strong> &nbsp; What accountability looks like week to week</p>
      <p style="{_S['bullet']}"><strong>Day 7</strong> &nbsp; One last thing to make it official</p>
    </div>

    <p style="{_S['p']}">
      Everything you need is also in the full onboarding portal. 13 sections, 13 videos, self-paced.
    </p>

    <div style="{_S['cta_wrap']}">
      <a href="{GAMMA_SITE}" style="display:inline-block;background:#1a1a2e;color:#ffffff;font-size:15px;font-weight:600;text-decoration:none;padding:13px 32px;border-radius:8px;">View Onboarding Portal →</a>
    </div>

    <p style="{_S['p']}">
      If you have questions at any point, reply here or text me directly. I'm glad you're here.
    </p>

    <p style="{_S['p']}">Barry<br>Legacy Home Team</p>
  </div>

  <div style="{_S['footer']}">
    Legacy Home Team &nbsp;·&nbsp; Your goal setup link is personal. Don't share it.
  </div>

</div>
</body></html>"""


def send_goal_onboarding_reminder(agent_name, first_name, email, setup_url, day=3):
    """
    Follow-up reminder for agents who haven't completed goal setup.
    day=3: gentle nudge
    day=7: more direct, mentions Barry is watching
    """
    if day == 3:
        subject = f"{first_name}, still waiting on your goals (2 min setup)"
        body = f"""Hey {first_name},

Just a quick follow-up — your personal goal dashboard is set up and ready, but I'm missing your numbers.

It's a 2-minute form. Here's your link:
{setup_url}

Once you set your goal, the system calculates your daily call target automatically and tracks your progress all year.

— Barry"""
    else:  # day 7
        subject = f"{first_name} — last nudge on this (your goal link)"
        body = f"""Hey {first_name},

I don't want to keep pinging you, but this matters.

Every agent who set a clear income goal last January outperformed every agent who didn't. It's not a coincidence.

Your personal setup link (takes 2 minutes):
{setup_url}

If you've got questions or want to talk through your goal, reply to this email or text me.

— Barry"""

    try:
        _pm.send(
            to=email,
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=body.replace("\n", "<br>"),
            text=body,
        )
        print(f"[ONBOARDING REMINDER] Day {day} sent to {agent_name} <{email}>")
        return True
    except Exception as e:
        print(f"[ONBOARDING REMINDER] Failed for {agent_name}: {e}")
        return False


def send_fast_track_invite_email(first_name, email, daily_dials, magic_link):
    """Course invite sent automatically after an agent completes goal setting and
    Command Center syncs it to Fast Track. Plain text, Barry's voice, no dashes.
    Returns True on success (so a send failure triggers a sync retry)."""
    subject = "Your Fast Track to Revenue starts here"
    dd = int(round(daily_dials)) if daily_dials else None
    standard_line = f"Your daily standard is {dd} dials. " if dd else ""
    body = f"""{first_name},

Your goal is set. {standard_line}Now it is time to learn the system that turns those dials into closings.

Fast Track to Revenue is a 10-day sprint that builds two skills, your contact rate and your ask, on the same system this team runs. Seven modules across ten days, then the Legacy Team Meeting, every week, forever.

Click below to enter. The course already knows the goal you just set, and everything in it will be graded against your own numbers, not a team average.

{magic_link}

If you have any questions, text or call me directly.

Barry"""
    try:
        _pm.send(
            to=email,
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=body.replace("\n", "<br>"),
            text=body,
            cc=[config.BARRY_EMAIL, config.MANAGER_EMAIL],
        )
        print(f"[FAST TRACK INVITE] Sent to {first_name} <{email}>")
        return True
    except Exception as e:
        print(f"[FAST TRACK INVITE] Failed for {email}: {e}")
        return False


def send_goal_setup_nudge_email(agent_name, first_name, email, setup_url):
    """Goal-setup outreach for an ESTABLISHED agent who hasn't set a goal yet.
    Different tone than the new-hire onboarding emails: respects that they're
    already producing, frames the goal as the thing that makes the work add up.
    Sent on a cadence by scheduled_goal_setup_outreach until the goal is set.
    CCs Barry + Joe so leadership sees the nudge went out."""
    subject = f"{first_name}, you're putting in the work. Let's aim it."
    body = f"""Hey {first_name},

You're making calls and doing the work, I see it. But right now you're doing it without a number to aim at, and that's the one thing holding you back from your best year.

When you set a real income goal, the system turns it into a daily target and tracks your pace all year. It's the difference between working hard and working toward something. The agents who set the number outperform the ones who don't, every time.

Takes 2 minutes. Here's your link:
{setup_url}

Set it and I'll make sure the whole system is working for you. Any questions, reply here or text me.

Barry"""
    try:
        _pm.send(
            to=email,
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=body.replace("\n", "<br>"),
            text=body,
            cc=[config.BARRY_EMAIL, config.MANAGER_EMAIL],
        )
        print(f"[GOAL OUTREACH] Nudge sent to {agent_name} <{email}>")
        return True
    except Exception as e:
        print(f"[GOAL OUTREACH] Failed for {agent_name}: {e}")
        return False


def send_goal_onboarding_email(agent_name, first_name, email, setup_url, dashboard_url=None):
    """
    Send the goal setup onboarding email to a new agent.
    Triggered automatically by the FUB roster sync when a new agent is detected.
    """
    html_body = build_goal_onboarding_email(first_name, setup_url, dashboard_url=dashboard_url)
    subject = f"{first_name}, welcome to Legacy Home Team. Start here."

    try:
        _pm.send(
            to=email,
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html_body,
            cc=[config.BARRY_EMAIL, config.MANAGER_EMAIL],
        )
        print(f"[ONBOARDING EMAIL] ✅ Sent to {agent_name} <{email}>")
        return True
    except Exception as e:
        print(f"[ONBOARDING EMAIL] ❌ Failed for {agent_name}: {e}")
        return False


# ---------------------------------------------------------------------------
# Onboarding sequence — Days 2 through 7
# ---------------------------------------------------------------------------

GAMMA_SITE = "https://legacy-home-team-dwurtn4.gamma.site/"

_ONBOARD_SEQ = {
    2: {
        "subject": "You picked a good team. Here's what that actually means.",
        "body": lambda f, setup_url: f"""Hey {f},

Most agents join a team and spend the first week trying to figure out the vibe. Let me just tell you directly.

This team runs on three things: gratitude for the opportunity in front of you, intentionality about where your time goes, and urgency when it comes to serving people.

That's it. No hype. No fake energy. Just a system that works when you work it.

Here's the one thing I want you to carry with you from day one: consistency beats intensity. Five real conversations a week, every single week, will outperform a burst of 30 calls followed by two weeks of silence. Every time. The whole system is built around that idea.

It's all in one place.

Full onboarding portal: {GAMMA_SITE}

13 sections, 13 videos. It won't take long. And it answers most of the questions you probably already have.

If you haven't set up your goals yet, your personal link is here: {setup_url}

Tomorrow I'll walk you through exactly how you get paid.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },

    3: {
        "subject": "Here's exactly how you get paid. No fine print.",
        "body": lambda f, setup_url: f"""Hey {f},

I've seen agents join teams and go months without really understanding their commission structure. That's a problem I don't want you to have.

Here it is, plain and clear.

The $595 Transaction Fee
Every closing has a $595 client-paid fee. It does not come out of your commission. You make sure it shows up on the settlement statement and that's it.

Your Commission Split
Almost everyone on the team is on the LPT Brokerage Partner Plan. Here's how it works:

80/20 split with a $15,000 annual cap. Once you hit that cap, you keep 100% for the rest of the year. There's a $500 annual fee withheld from your first deal (covers technology and E&O). No monthly fees, no sign-up fees.

Do the math: if your average commission check is $6,000 per deal, you hit the cap around deal 3 or 4. From that point forward, everything is yours.

There's also a Business Builder option ($500 flat per transaction, $5,000 cap, 100% from day one) that works better for high-volume agents focused on rapid growth. Full comparison is in Section 8 of the onboarding portal.

What the team covers: leads, FUB CRM, AI engagement, ISA support, weekly training, 1:1 coaching with Joe, office access, and all LPT tools.

What you cover: your license, dues, CE, mileage, and personal marketing.

Tomorrow: the daily workflow that organizes your whole day.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },

    4: {
        "subject": "Every morning starts with one click. Here's what to do with it.",
        "body": lambda f, setup_url: f"""Hey {f},

When you open Follow Up Boss and click All People, you're going to see a collection called Start Here.

That's your command center. Every single day.

It's broken into four categories. Work them in this order.

1. LeadStream. Your leads, ranked for you.
These are your assigned leads, sorted from highest LeadStream score to lowest. The score pulls from website activity, recency, stage, and over 40 other signals. The person at the top of that list is the first call you make. You don't have to guess who to call. The system already decided. Trust it.

2. LeadStream Pond. Leads up for grabs.
Unassigned leads showing activity or meeting criteria worth your attention. Nobody owns them yet. Click View All Ponds to see the full list. When you make contact with one and they engage, claim them and put them in your name. First come, first served.

3. Out of Compliance. Leads you've let fall behind.
This is the Maverick system doing its job. Someone texted you back and you haven't responded. A lead is in Hot stage and you haven't touched them recently. These are the ones. Clear the list by logging a call or sending a text. If this list keeps growing, we're going to have a conversation. Not a punishing one. A real one about what's getting in the way.

4. Appointments: Missing Outcomes.
If you set an appointment and didn't log what happened, it shows up here. Did you meet? Did they cancel? No-show? Click the appointment and record it. We track outcomes to coach you effectively.

That's the whole workflow. Open Start Here every morning. Work your LeadStream list top to bottom. Fish the pond. Clear compliance. Update your appointment outcomes.

That single habit, done consistently, will materially change your results. I've seen it happen.

Full walkthrough is in Section 4 of the portal.

Tomorrow: getting your LPT account fully set up.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },

    5: {
        "subject": "10 things to do this week at LPT. Start with number one.",
        "body": lambda f, setup_url: f"""Hey {f},

LPT is a great brokerage. Cloud-based, consistently ranked top 10 nationally, and built around the agent. But like anything, it only works if you actually set it up.

Here are the 10 things to complete in your first week. Don't let these sit.

1. Complete Automated Systems Setup
2. Schedule your Zoom Orientation (calendly.com/lpt-realty-orientation)
3. Submit a sample support ticket (support@lptrealty.com)
4. Activate Listing Power Tools
5. Access the Knowledge Base
6. Build your Desi Designer marketing profile
7. Register for the Training Library
8. Complete a test Dotloop
9. Register for Motivational Monday (Mondays at 11am ET)
10. Post an introduction in the Community Forum

The orientation is the most important one. Get it on your calendar today.

Questions about LPT? Call them at 1-877-366-2213 or email info@lptrealty.com.

Everything else is in Section 10 of the portal.

One more email tomorrow. Last step before you're fully official.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },

    6: {
        "subject": "This is what the rhythm looks like from here.",
        "body": lambda f, setup_url: f"""Hey {f},

You've been through the handbook. You know how leads work, how you get paid, and what your daily system looks like.

Here's what the rhythm looks like going forward.

Every week Joe is going to sit down with you and go through your numbers. Calls, conversations, pipeline movement. Come ready to be honest about what's working and what isn't. That conversation is not a performance review. It's the whole point of being on this team.

Reach Joe at (757) 286-7819 for your weekly 1:1s.

The Maverick system will flag leads that need attention. When it does, log a call or a text and it clears. No drama. Just stay on top of it.

If you ever get stuck on anything, text Joe. Anything urgent, text me at (757) 816-4037.

One last thing coming tomorrow. It takes two minutes.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },

    7: {
        "subject": "One signature and you're fully official.",
        "body": lambda f, setup_url: f"""Hey {f},

You've made it through the handbook. You've seen how we operate, how you get paid, how to work your leads, and how to get set up at LPT.

One thing left.

Go to the onboarding portal, find the handbook, and sign the last page. It's a signature page acknowledging you've read and understood how we work together. Email the signed copy to barry@yourfriendlyagent.net.

That's it. Two minutes.

I don't ask for this to be bureaucratic. I ask for it because this team runs on clarity and mutual commitment. You know what's expected. I know you've seen it. That signature closes the loop.

Once I have it, you're fully in.

Looking forward to building something with you.

Barry
Legacy Home Team

Full onboarding portal: {GAMMA_SITE}""",
    },
}


def send_onboarding_sequence_email(agent_name, first_name, email, setup_url, day):
    """
    Send one email from the 6-part onboarding content sequence (days 2-7).
    Day 1 is the goal setup invite (send_goal_onboarding_email).
    Days 2-7 are content emails covering culture, compensation, smart lists,
    LPT onboarding, accountability rhythm, and handbook signature.

    day: int 2-7 corresponding to days since onboarding started.
    """
    seq = _ONBOARD_SEQ.get(day)
    if not seq:
        print(f"[ONBOARDING SEQ] No email defined for day {day}")
        return False

    subject = seq["subject"]
    body    = seq["body"](first_name, setup_url)
    html    = body.replace("\n", "<br>")

    try:
        _pm.send(
            to=email,
            from_email=config.EMAIL_FROM,
            subject=subject,
            html=html,
            text=body,
            cc=[config.BARRY_EMAIL, config.MANAGER_EMAIL],
        )
        print(f"[ONBOARDING SEQ] Day {day} sent to {agent_name} <{email}>")
        return True
    except Exception as e:
        print(f"[ONBOARDING SEQ] Day {day} failed for {agent_name}: {e}")
        return False


# ---------------------------------------------------------------------------
# Impact Tracker brief — emailed to Barry the moment Joe submits
# ---------------------------------------------------------------------------

_STATUS_LABEL = {"thriving": "Thriving", "steady": "Steady",
                 "struggling": "Struggling", "needs": "Needs you"}
_STATUS_COLOR = {"thriving": "#1d9e75", "steady": "#185fa5",
                 "struggling": "#854f0b", "needs": "#a32d2d"}
_GRADE_COLOR = {"A": "#1d9e75", "B": "#1d9e75", "C": "#854f0b",
                "D": "#a32d2d", "F": "#a32d2d"}


def build_impact_tracker_email(date_label, entries, stats, analytics, insight):
    """Owner-first HTML brief for Barry, built around three questions:
    is Joe doing his job, where is the business leaking, what did Joe report."""
    stats = stats or {}
    analytics = analytics or {}
    met = [e for e in entries if e.get("met") == "yes"]
    joe = analytics.get("joe", {}) or {}
    tf = (analytics.get("team_funnel", {}) or {}).get("agents", []) or []

    # --- Q1: is Joe doing his job? ---
    n_met = joe.get("met_this_week", len(met))
    hitting = joe.get("hitting", n_met >= 3)
    j_col = "#0f6e56" if hitting else "#a32d2d"
    j_bg = "#e1f5ee" if hitting else "#fcebeb"
    j_line = ("Joe hit his number of 1:1s this week." if hitting
              else f"You asked for 3 to 4. Joe met {n_met}. He is short.")
    q1 = (f'<div style="background:{j_bg};border-radius:10px;padding:16px 20px;margin:0 0 22px">'
          f'<div style="font-size:12px;font-weight:700;color:#5f5e5a;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px">1. Is Joe doing his job?</div>'
          f'<div style="font-size:28px;font-weight:800;color:{j_col};line-height:1">{n_met} of 3 to 4</div>'
          f'<div style="font-size:14px;color:#2d3748;margin-top:5px;font-weight:600">{j_line}</div></div>')

    # --- Q2: where is the business leaking? ---
    total_c = sum(a.get("calls", 0) for a in tf)
    total_a = sum(a.get("appts", 0) for a in tf)
    leakers = [a for a in tf if a.get("calls", 0) >= 40 and a.get("appts", 0) <= 1]
    def _read(a):
        lk = a.get("leak")
        if lk == "idle":     return ("Not working", "#a32d2d")
        if lk == "no_appts": return ("Calling, zero booked", "#a32d2d")
        if lk == "low_appts":return ("Lots of calls, few booked", "#854f0b")
        return ("On track", "#0f6e56")
    rows = ""
    for a in tf:
        txt, c = _read(a)
        rows += (f'<tr><td style="padding:8px 6px;border-bottom:1px solid #edf0f4"><strong>{a.get("agent","")}</strong></td>'
                 f'<td style="padding:8px 6px;border-bottom:1px solid #edf0f4;text-align:right">{a.get("calls",0)}</td>'
                 f'<td style="padding:8px 6px;border-bottom:1px solid #edf0f4;text-align:right">{a.get("appts",0)}</td>'
                 f'<td style="padding:8px 6px;border-bottom:1px solid #edf0f4;text-align:right;font-weight:700;color:{c}">{txt}</td></tr>')
    leak_line = (f'<strong style="color:#a32d2d">{len(leakers)} agents dialed hard and barely booked.</strong> That is the leak.'
                 if leakers else "")
    q2 = (f'<div style="margin:0 0 22px">'
          f'<div style="font-size:12px;font-weight:700;color:#5f5e5a;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">2. Where is the money leaking?</div>'
          f'<div style="font-size:17px;font-weight:700;color:#1a1a2e;margin-bottom:6px">Calls are fine. Appointments are the leak.</div>'
          f'<p style="font-size:14px;color:#2d3748;margin:0 0 12px">Last full week the team made <strong>{total_c} calls</strong> and set <strong>{total_a} appointments</strong>. {leak_line}</p>'
          f'<table style="width:100%;border-collapse:collapse;font-size:13px;color:#2d3748">'
          f'<tr><td style="padding:5px 6px;font-size:11px;color:#a0aec0;text-transform:uppercase;letter-spacing:.05em">Agent</td>'
          f'<td style="padding:5px 6px;font-size:11px;color:#a0aec0;text-align:right;text-transform:uppercase">Calls</td>'
          f'<td style="padding:5px 6px;font-size:11px;color:#a0aec0;text-align:right;text-transform:uppercase">Appts</td>'
          f'<td style="padding:5px 6px;font-size:11px;color:#a0aec0;text-align:right;text-transform:uppercase">Read</td></tr>'
          f'{rows}</table>'
          f'<p style="font-size:12px;color:#718096;margin:10px 0 0">Under-contract tracking is not reliable yet (Dotloop sync pending), so this is calls and appointments only, which are exact.</p></div>')

    # --- Q3: what Joe reported ---
    def _chip(txt, color):
        return (f'<span style="display:inline-block;background:{color};color:#fff;border-radius:5px;'
                f'padding:1px 7px;font-size:12px;font-weight:700">{txt}</span>')
    mrows = ""
    for e in met:
        ag = e.get("agent", "")
        status = e.get("status")
        schip = _chip(_STATUS_LABEL.get(status, ""), _STATUS_COLOR.get(status, "#5f5e5a")) if status else ""
        commit = e.get("commit") or "<span style='color:#a0aec0'>no commitment logged</span>"
        mrows += (f'<tr><td style="padding:10px 8px;border-bottom:1px solid #edf0f4">'
                  f'<strong style="font-size:14px">{ag}</strong> {schip}'
                  f'<div style="font-size:13px;color:#2d3748;margin-top:4px"><strong>Committed to:</strong> {commit}</div></td></tr>')
    q3 = (f'<div style="margin:0 0 8px">'
          f'<div style="font-size:12px;font-weight:700;color:#5f5e5a;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">3. What Joe reported</div>'
          + (f'<table style="width:100%;border-collapse:collapse">{mrows}</table>'
             if mrows else '<p style="color:#a0aec0;font-size:14px">No meetings logged this week.</p>')
          + '</div>')

    base = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")
    full_link = f"{base}/sales-manager?key=lht-perp-2026"

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<div style="max-width:600px;margin:24px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.08)">
  <div style="background:#16213e;padding:22px 28px">
    <div style="color:#fff;font-size:19px;font-weight:700">Impact Tracker</div>
    <div style="color:#9fb0c8;font-size:13px;margin-top:3px">Joe &middot; {date_label}</div>
  </div>
  <div style="padding:24px 28px">
    {q1}
    {q2}
    {q3}
    <div style="text-align:center;margin:24px 0 6px">
      <a href="{full_link}" style="display:inline-block;background:#0f6e56;color:#fff;text-decoration:none;
         padding:13px 28px;border-radius:8px;font-weight:700;font-size:14px">Open full Sales Manager view</a>
    </div>
  </div>
</div></body></html>"""


def send_impact_tracker_brief(date_label, entries, stats, analytics, dry_run=False):
    """Build + send the Impact Tracker brief to Barry. Generates the AI insight.
    dry_run=True builds the full email (exercising render) but does not send,
    so the whole pipeline can be self-tested without emailing anyone."""
    try:
        import coach_voice
        insight = coach_voice.generate_manager_brief({
            "this_week": entries,
            "accountability": (analytics or {}).get("accountability"),
            "impact": (analytics or {}).get("impact"),
            "coaching_gaps": (analytics or {}).get("coverage", {}).get("gaps"),
        })
    except Exception:
        insight = None
    html = build_impact_tracker_email(date_label, entries, stats, analytics, insight)
    met = sum(1 for e in entries if e.get("met") == "yes")
    flagged = sum(1 for e in entries if e.get("met") == "yes" and e.get("status") == "needs")
    subject = f"Impact Tracker: Joe met {met}" + (f", {flagged} need you" if flagged else "")
    if dry_run:
        return True   # built successfully; caller is self-testing
    _pm.send(to=config.BARRY_EMAIL, from_email=config.EMAIL_FROM,
             subject=subject, html=html)
    print(f"[IMPACT TRACKER] Brief emailed to Barry ({met} met)")
    return True
