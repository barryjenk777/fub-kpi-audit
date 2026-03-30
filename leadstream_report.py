"""
LeadStream Daily Report — Agent Activity Accountability

Checks which LeadStream-tagged leads were actually contacted by agents
and emails a morning recap to the team lead.

Usage:
    python leadstream_report.py              # Send daily report
    python leadstream_report.py --preview    # Print report without emailing
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content

from config import (
    EXCLUDED_USERS,
    LEADSTREAM_TAG,
    LEADSTREAM_POND_TAG,
)
from fub_client import FUBClient

# ── Config ──────────────────────────────────────────────────────────
REPORT_RECIPIENT = "barry@yourfriendlyagent.net"
REPORT_FROM = "barry@yourfriendlyagent.net"  # Must be verified in SendGrid
MANIFEST_FILE = os.path.join(os.path.dirname(__file__), ".cache", "leadstream_manifest.json")
HISTORY_DIR = os.path.join(os.path.dirname(__file__), ".cache", "leadstream_history")


def _get_client():
    """Create a FUB client, preferring the LeadStream-specific key."""
    from config import LEADSTREAM_API_KEY_ENV
    api_key = os.getenv(LEADSTREAM_API_KEY_ENV) or os.getenv("FUB_API_KEY")
    if not api_key:
        raise ValueError("No FUB API key found")
    return FUBClient(api_key=api_key)


def _load_manifest():
    """Load the current manifest of tagged lead IDs."""
    try:
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"agent": {}, "pond": []}


def _save_history(report_date, report_data):
    """Save daily report to history for trend tracking."""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    path = os.path.join(HISTORY_DIR, f"{report_date}.json")
    with open(path, "w") as f:
        json.dump(report_data, f, indent=2)


def _get_contacted_person_ids(client, since):
    """Get set of personIds that received outbound calls or texts since a timestamp."""
    contacted = {}

    # Outbound calls
    try:
        calls = client.get_calls(since=since)
        for call in calls:
            if not call.get("isIncoming"):
                pid = call.get("personId")
                uid = call.get("userId")
                if pid:
                    contacted[pid] = {
                        "method": "call",
                        "agentId": uid,
                        "time": call.get("created"),
                    }
    except Exception:
        pass

    # Outbound texts
    try:
        texts = client.get_text_messages(since=since)
        for text in texts:
            if text.get("isOutbound"):
                pid = text.get("personId")
                uid = text.get("userId")
                if pid and pid not in contacted:
                    contacted[pid] = {
                        "method": "text",
                        "agentId": uid,
                        "time": text.get("created"),
                    }
    except Exception:
        pass

    return contacted


def _get_agent_map(client):
    """Build userId -> name map for all agents."""
    users = client.get_users()
    return {
        u["id"]: f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
        for u in users
    }


def build_report(client, preview=False):
    """Build the daily LeadStream activity report."""
    manifest = _load_manifest()
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(hours=24)
    report_date = (now - timedelta(hours=8)).strftime("%Y-%m-%d")  # Adjust for timezone

    print(f"Building LeadStream report for {report_date}...")

    # Get all outbound contacts in the last 24h
    contacted = _get_contacted_person_ids(client, yesterday)
    agent_map = _get_agent_map(client)

    report_data = {
        "date": report_date,
        "agents": {},
        "pond": {"tagged": 0, "actioned": 0, "leads": []},
        "totals": {"tagged": 0, "actioned": 0, "action_rate": 0},
    }

    # ── Agent Leads ─────────────────────────────────────────────────
    for agent_name, lead_items in manifest.get("agent", {}).items():
        agent_result = {
            "tagged": len(lead_items),
            "actioned": 0,
            "not_actioned": [],
            "actioned_leads": [],
        }

        for item in lead_items:
            pid = item["id"] if isinstance(item, dict) else item
            # Use stored name if available, else look up
            if isinstance(item, dict) and item.get("name"):
                lead_name = item["name"]
            else:
                try:
                    person = client.get_person(pid)
                    lead_name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
                except Exception:
                    lead_name = f"ID:{pid}"

            if pid in contacted:
                agent_result["actioned"] += 1
                agent_result["actioned_leads"].append({
                    "id": pid,
                    "name": lead_name,
                    "method": contacted[pid]["method"],
                })
            else:
                agent_result["not_actioned"].append({
                    "id": pid,
                    "name": lead_name,
                })

        report_data["agents"][agent_name] = agent_result

    # ── Pond Leads ──────────────────────────────────────────────────
    pond_items = manifest.get("pond", [])
    report_data["pond"]["tagged"] = len(pond_items)

    for item in pond_items:
        pid = item["id"] if isinstance(item, dict) else item
        if isinstance(item, dict) and item.get("name"):
            lead_name = item["name"]
        else:
            try:
                person = client.get_person(pid)
                lead_name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
            except Exception:
                lead_name = f"ID:{pid}"

        if pid in contacted:
            report_data["pond"]["actioned"] += 1
            claimed_by = agent_map.get(contacted[pid].get("agentId"), "Unknown")
            report_data["pond"]["leads"].append({
                "id": pid,
                "name": lead_name,
                "status": "actioned",
                "claimed_by": claimed_by,
                "method": contacted[pid]["method"],
            })
        else:
            report_data["pond"]["leads"].append({
                "id": pid,
                "name": lead_name,
                "status": "not_actioned",
            })

    # ── Totals ──────────────────────────────────────────────────────
    total_tagged = sum(a["tagged"] for a in report_data["agents"].values()) + report_data["pond"]["tagged"]
    total_actioned = sum(a["actioned"] for a in report_data["agents"].values()) + report_data["pond"]["actioned"]
    action_rate = (total_actioned / total_tagged * 100) if total_tagged > 0 else 0

    report_data["totals"] = {
        "tagged": total_tagged,
        "actioned": total_actioned,
        "action_rate": round(action_rate, 1),
    }

    # Save history
    _save_history(report_date, report_data)

    return report_data


def format_email_html(data):
    """Format the report data as an HTML email."""
    date = data["date"]
    totals = data["totals"]

    # Color for action rate
    rate = totals["action_rate"]
    if rate >= 80:
        rate_color = "#22c55e"  # green
    elif rate >= 50:
        rate_color = "#f59e0b"  # amber
    else:
        rate_color = "#ef4444"  # red

    html = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #1a1a1a;">

    <h1 style="font-size: 24px; margin-bottom: 5px;">📊 LeadStream Daily Report</h1>
    <p style="color: #666; margin-top: 0;">{date}</p>

    <!-- Summary Banner -->
    <div style="background: #f8fafc; border-radius: 12px; padding: 20px; margin: 20px 0; display: flex; border: 1px solid #e2e8f0;">
        <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
            <td align="center" width="33%">
                <div style="font-size: 32px; font-weight: bold;">{totals['tagged']}</div>
                <div style="color: #666; font-size: 13px;">Leads Tagged</div>
            </td>
            <td align="center" width="33%">
                <div style="font-size: 32px; font-weight: bold;">{totals['actioned']}</div>
                <div style="color: #666; font-size: 13px;">Leads Actioned</div>
            </td>
            <td align="center" width="33%">
                <div style="font-size: 32px; font-weight: bold; color: {rate_color};">{totals['action_rate']}%</div>
                <div style="color: #666; font-size: 13px;">Action Rate</div>
            </td>
        </tr>
        </table>
    </div>

    <!-- Agent Breakdown -->
    <h2 style="font-size: 18px; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px;">👤 Agent Performance</h2>
    <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 14px;">
    <tr style="background: #f1f5f9;">
        <th align="left" style="padding: 10px; border-bottom: 2px solid #e2e8f0;">Agent</th>
        <th align="center" style="padding: 10px; border-bottom: 2px solid #e2e8f0;">Tagged</th>
        <th align="center" style="padding: 10px; border-bottom: 2px solid #e2e8f0;">Actioned</th>
        <th align="center" style="padding: 10px; border-bottom: 2px solid #e2e8f0;">Missed</th>
        <th align="center" style="padding: 10px; border-bottom: 2px solid #e2e8f0;">Rate</th>
    </tr>
    """

    for agent_name, agent_data in sorted(data["agents"].items()):
        tagged = agent_data["tagged"]
        actioned = agent_data["actioned"]
        missed = tagged - actioned
        agent_rate = (actioned / tagged * 100) if tagged > 0 else 0

        if agent_rate >= 80:
            badge = "🟢"
        elif agent_rate >= 50:
            badge = "🟡"
        else:
            badge = "🔴"

        html += f"""
    <tr style="border-bottom: 1px solid #e2e8f0;">
        <td style="padding: 10px;">{agent_name}</td>
        <td align="center" style="padding: 10px;">{tagged}</td>
        <td align="center" style="padding: 10px;">{actioned}</td>
        <td align="center" style="padding: 10px; color: {'#ef4444' if missed > 0 else '#22c55e'};">{missed}</td>
        <td align="center" style="padding: 10px;">{badge} {agent_rate:.0f}%</td>
    </tr>
    """

        # Show missed leads (not actioned)
        if agent_data["not_actioned"]:
            missed_names = ", ".join(l["name"] for l in agent_data["not_actioned"][:5])
            more = len(agent_data["not_actioned"]) - 5
            more_text = f" +{more} more" if more > 0 else ""
            html += f"""
    <tr>
        <td colspan="5" style="padding: 4px 10px 10px 30px; color: #888; font-size: 12px;">
            ❌ Not contacted: {missed_names}{more_text}
        </td>
    </tr>
    """

    html += "</table>"

    # ── Pond Section ────────────────────────────────────────────────
    pond = data["pond"]
    if pond["tagged"] > 0:
        html += f"""
    <h2 style="font-size: 18px; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; margin-top: 30px;">🦈 Shark Tank (Pond)</h2>
    <p style="color: #666; font-size: 14px;">{pond['actioned']} of {pond['tagged']} pond leads were claimed and contacted</p>
    <table width="100%" cellpadding="6" cellspacing="0" style="border-collapse: collapse; font-size: 13px;">
    """
        for lead in pond["leads"]:
            if lead["status"] == "actioned":
                html += f"""
    <tr style="border-bottom: 1px solid #f1f5f9;">
        <td style="padding: 6px;">✅ {lead['name']}</td>
        <td style="padding: 6px; color: #666;">Claimed by {lead['claimed_by']} ({lead['method']})</td>
    </tr>
    """
            else:
                html += f"""
    <tr style="border-bottom: 1px solid #f1f5f9;">
        <td style="padding: 6px;">⬜ {lead['name']}</td>
        <td style="padding: 6px; color: #aaa;">Not contacted</td>
    </tr>
    """
        html += "</table>"

    html += """
    <hr style="margin-top: 30px; border: none; border-top: 1px solid #e2e8f0;">
    <p style="color: #aaa; font-size: 12px; text-align: center;">
        LeadStream Daily Report — Auto-generated from Follow Up Boss data
    </p>
    </body>
    </html>
    """

    return html


def format_text_report(data):
    """Format report as plain text for console preview."""
    lines = []
    date = data["date"]
    totals = data["totals"]

    lines.append(f"\n{'='*60}")
    lines.append(f"  LeadStream Daily Report — {date}")
    lines.append(f"{'='*60}")
    lines.append(f"\n  Total: {totals['tagged']} tagged → {totals['actioned']} actioned ({totals['action_rate']}%)\n")

    lines.append(f"  {'Agent':<25} {'Tagged':>7} {'Done':>7} {'Missed':>7} {'Rate':>7}")
    lines.append(f"  {'-'*53}")

    for agent_name, d in sorted(data["agents"].items()):
        tagged = d["tagged"]
        actioned = d["actioned"]
        missed = tagged - actioned
        rate = (actioned / tagged * 100) if tagged > 0 else 0
        lines.append(f"  {agent_name:<25} {tagged:>7} {actioned:>7} {missed:>7} {rate:>6.0f}%")

        if d["not_actioned"]:
            for lead in d["not_actioned"]:
                lines.append(f"    ❌ {lead['name']}")

    pond = data["pond"]
    if pond["tagged"] > 0:
        lines.append(f"\n  Shark Tank: {pond['actioned']}/{pond['tagged']} claimed")
        for lead in pond["leads"]:
            if lead["status"] == "actioned":
                lines.append(f"    ✅ {lead['name']} → {lead['claimed_by']} ({lead['method']})")
            else:
                lines.append(f"    ⬜ {lead['name']}")

    lines.append(f"\n{'='*60}\n")
    return "\n".join(lines)


def send_email(html_content, report_date):
    """Send the report via SendGrid."""
    sg_key = os.getenv("SENDGRID_API_KEY")
    if not sg_key:
        raise ValueError("SENDGRID_API_KEY not set in environment")

    message = Mail(
        from_email=REPORT_FROM,
        to_emails=REPORT_RECIPIENT,
        subject=f"LeadStream Report — {report_date}",
        html_content=html_content,
    )

    sg = SendGridAPIClient(sg_key)
    response = sg.send(message)
    return response.status_code


def main():
    parser = argparse.ArgumentParser(description="LeadStream Daily Report")
    parser.add_argument("--preview", action="store_true",
                        help="Preview report in console without sending email")
    args = parser.parse_args()

    # Load env if available
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

    client = _get_client()
    report_data = build_report(client)

    # Console output
    print(format_text_report(report_data))

    if args.preview:
        print("(Preview mode — email not sent)")
        return

    # Send email
    try:
        html = format_email_html(report_data)
        status = send_email(html, report_data["date"])
        print(f"Email sent to {REPORT_RECIPIENT} (status: {status})")
    except Exception as e:
        print(f"Email send failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
