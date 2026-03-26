"""
Send the weekly KPI audit report via SendGrid email.
Designed as a team leader weekly update with actionable insights.

Requires: pip install sendgrid
           export SENDGRID_API_KEY="SG.your_key_here"
"""

import os
from datetime import datetime

import config


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
    subject = f"Weekly Team Update — {period} — {passed}/{total} passed KPIs"

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
