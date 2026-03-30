"""
Send the weekly KPI audit report via SendGrid email.
Designed as a team leader weekly update with actionable insights.

Requires: pip install sendgrid
           export SENDGRID_API_KEY="SG.your_key_here"
"""

import os
import random
from datetime import datetime

import config


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

    <h1>Weekly Team Update</h1>
    <p style="color:#666; margin-top:0;">{period} &nbsp;|&nbsp; Thresholds: Calls &ge;{config.MIN_OUTBOUND_CALLS} &bull; Convos &ge;{config.MIN_CONVERSATIONS} &bull; OOC &le;{config.MAX_OUT_OF_COMPLIANCE}</p>
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
    html += '<h2>Action Items This Week</h2>'

    # Live Calls inbox
    html += f"""
    <div class="action-box">
        <strong>@{admin}:</strong> Please update the <strong>Live Calls</strong> inbox in Follow Up Boss:<br><br>
    """
    if passed:
        html += "<strong>ADD</strong> these agents:<br>"
        for name in passed:
            html += f'&nbsp;&nbsp;&nbsp;&nbsp;&#9989; {name}<br>'
    if failed:
        if passed:
            html += "<br>"
        html += "<strong>REMOVE</strong> these agents:<br>"
        for name in failed:
            html += f'&nbsp;&nbsp;&nbsp;&nbsp;&#10060; {name}<br>'
    html += "</div>"

    # Priority group status
    if passed:
        html += f"""
        <div class="action-box green">
            <strong>Priority Agents Group</strong> (auto-updated when run with --update-group):<br>
            {', '.join(passed)}
        </div>"""
    else:
        html += """
        <div class="action-box red">
            <strong>Priority Agents Group:</strong> No agents qualified this week. All new leads go to the pond.
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

    <h1>{emoji} Monday Game Plan — {period_label}</h1>
    <p style="color:#666;margin-top:0;">KPI Targets: Calls ≥{kpi['min_calls']} • Convos ≥{kpi['min_convos']} • OOC ≤{kpi['max_ooc']}</p>

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
    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        print("\n⚠  SENDGRID_API_KEY not set.")
        return False

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, To
    except ImportError:
        return False

    html_body = build_manager_email(manager_data, period_label)
    cs = manager_data["coaching_summary"]
    meeting = cs["meeting_kpi"]
    total = cs["total_agents"]

    subject = _catchy_subject("manager", {"meeting": meeting, "total": total})

    # Joe + Barry (CC)
    recipients = [
        getattr(config, "MANAGER_EMAIL", "thejoefu@gmail.com"),
    ] + list(config.EMAIL_RECIPIENTS)
    # Deduplicate
    seen = set()
    unique = []
    for e in recipients:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    to_list = [To(email) for email in unique]

    message = Mail(
        from_email=config.EMAIL_FROM,
        to_emails=to_list,
        subject=subject,
        html_content=html_body,
    )

    try:
        sg = SendGridAPIClient(api_key)
        sg.send(message)
        print(f"\n✅ Manager email sent to {len(unique)} recipients")
        return True
    except Exception as e:
        print(f"\n❌ Failed to send manager email: {e}")
        return False


def send_report(results, period_start, period_end):
    """Send the audit report via SendGrid."""
    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        print("\n⚠  SENDGRID_API_KEY not set. Skipping email.")
        print("   Set it with: export SENDGRID_API_KEY=\"SG.your_key_here\"")
        return False

    if not config.EMAIL_RECIPIENTS:
        print("\n⚠  No EMAIL_RECIPIENTS configured. Skipping email.")
        return False

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, To
    except ImportError:
        print("\n⚠  sendgrid package not installed. Run: pip install sendgrid")
        return False

    html_body = build_html_report(results, period_start, period_end)
    period = f"{period_start.strftime('%b %d')} — {period_end.strftime('%b %d, %Y')}"

    passed = sum(1 for d in results.values() if d["evaluation"]["overall_pass"])
    total = len(results)
    subject = _catchy_subject("audit", {"passed": passed, "total": total})

    # Add Fhalen (Live Calls admin) to recipients if not already included
    all_recipients = list(config.EMAIL_RECIPIENTS)
    admin_email = getattr(config, "LIVE_CALLS_ADMIN_EMAIL", None)
    if admin_email and admin_email not in all_recipients:
        all_recipients.append(admin_email)

    to_list = [To(email) for email in all_recipients]

    message = Mail(
        from_email=config.EMAIL_FROM,
        to_emails=to_list,
        subject=subject,
        html_content=html_body,
    )

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
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

    <h1>{emoji} ISA Weekly Performance — {period}</h1>

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
    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        return False

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, To
    except ImportError:
        return False

    html_body = build_isa_email(isa_data)
    c = isa_data.get("current", {})
    subject = _catchy_subject("isa", {"calls": c.get("calls", 0), "convos": c.get("convos", 0)})

    recipients = list(config.EMAIL_RECIPIENTS)
    admin_email = getattr(config, "LIVE_CALLS_ADMIN_EMAIL", None)
    if admin_email and admin_email not in recipients:
        recipients.append(admin_email)

    seen = set()
    unique = []
    for e in recipients:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    message = Mail(
        from_email=config.EMAIL_FROM,
        to_emails=[To(e) for e in unique],
        subject=subject,
        html_content=html_body,
    )

    try:
        sg = SendGridAPIClient(api_key)
        sg.send(message)
        print(f"\n✅ ISA email sent to {len(unique)} recipients")
        return True
    except Exception as e:
        print(f"\n❌ Failed to send ISA email: {e}")
        return False
