#!/usr/bin/env python3
"""
Legacy Home Team — Weekly KPI Audit
Evaluates agent performance against KPIs and optionally
updates the Priority Agents group in Follow Up Boss.

Usage:
    python kpi_audit.py                          # Report only
    python kpi_audit.py --update-group           # Report + update FUB group
    python kpi_audit.py --min-calls 30           # Override call threshold
    python kpi_audit.py --min-calls 30 --min-convos 3 --max-ooc 5
    python kpi_audit.py --list-groups            # Show all FUB groups
    python kpi_audit.py --list-users             # Show all FUB users
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from fub_client import FUBClient
from email_report import send_report
import config


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Legacy Home Team KPI Audit"
    )
    parser.add_argument(
        "--update-group", action="store_true",
        help="Update the Priority Agents group in FUB based on results"
    )
    parser.add_argument(
        "--list-groups", action="store_true",
        help="List all FUB groups and their IDs"
    )
    parser.add_argument(
        "--list-users", action="store_true",
        help="List all FUB users and their IDs"
    )
    parser.add_argument(
        "--api-key", type=str, default=None,
        help="FUB API key (or set FUB_API_KEY env var)"
    )
    parser.add_argument(
        "--weeks-back", type=int, default=1,
        help="Number of weeks back to audit (default: 1)"
    )
    # KPI threshold overrides
    parser.add_argument(
        "--min-calls", type=int, default=None,
        help=f"Override minimum outbound calls (default: {config.MIN_OUTBOUND_CALLS})"
    )
    parser.add_argument(
        "--min-convos", type=int, default=None,
        help=f"Override minimum conversations (default: {config.MIN_CONVERSATIONS})"
    )
    parser.add_argument(
        "--max-ooc", type=int, default=None,
        help=f"Override max out-of-compliance leads (default: {config.MAX_OUT_OF_COMPLIANCE})"
    )
    parser.add_argument(
        "--email", action="store_true",
        help="Email the audit report to configured recipients"
    )
    return parser.parse_args()


def apply_threshold_overrides(args):
    """Apply CLI threshold overrides to config values for this run."""
    if args.min_calls is not None:
        config.MIN_OUTBOUND_CALLS = args.min_calls
    if args.min_convos is not None:
        config.MIN_CONVERSATIONS = args.min_convos
    if args.max_ooc is not None:
        config.MAX_OUT_OF_COMPLIANCE = args.max_ooc


# ---------------------------------------------------------------------------
# Agent auto-detection
# ---------------------------------------------------------------------------

def auto_detect_agents(client):
    """
    Auto-detect active agents from FUB, excluding configured users.
    Returns dict of {name: user_dict}.
    """
    all_users = client.get_users()
    excluded_lower = {n.lower() for n in config.EXCLUDED_USERS}

    agent_map = {}
    for user in all_users:
        full_name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
        status = (user.get("status") or "").lower()
        role = (user.get("role") or "").lower()

        # Only active agents
        if status != "active":
            continue
        if full_name.lower() in excluded_lower:
            continue
        # Include agents and brokers not in excluded list
        agent_map[full_name] = user

    return agent_map


# ---------------------------------------------------------------------------
# KPI metric collection
# ---------------------------------------------------------------------------

def count_calls_for_user(all_calls, user_id, excluded_person_ids=None):
    """
    Count outbound calls and conversations for an agent.
    Matches FUB's dashboard definitions exactly:

    Calls Made:    all outbound calls (isIncoming == False)
    Conversations: all calls (in + out) with duration >= threshold (default 120s)

    Excluded person IDs (e.g., Sphere leads) are not counted toward conversations.
    Returns (total_outbound, conversations, total_talk_seconds).
    """
    if excluded_person_ids is None:
        excluded_person_ids = set()

    threshold = config.CONVERSATION_THRESHOLD_SECONDS
    outbound = 0
    conversations = 0
    total_talk_seconds = 0

    for call in all_calls:
        if call.get("userId") != user_id:
            continue

        duration = call.get("duration", 0) or 0
        is_incoming = call.get("isIncoming", False)

        # --- Calls Made: all outbound ---
        if not is_incoming:
            outbound += 1

        # --- Conversations: any call (in or out) >= threshold ---
        if duration >= threshold:
            if call.get("personId") not in excluded_person_ids:
                conversations += 1
                total_talk_seconds += duration

    return outbound, conversations, total_talk_seconds


def build_excluded_person_ids(client, all_calls):
    """
    Fetch people records for conversation-eligible calls and return a set
    of personIds whose lead source is in EXCLUDED_LEAD_SOURCES.
    """
    if not config.EXCLUDED_LEAD_SOURCES:
        return set()

    excluded_lower = {s.lower() for s in config.EXCLUDED_LEAD_SOURCES}

    # Collect personIds from calls that could be conversations
    candidate_ids = set()
    for call in all_calls:
        if (call.get("duration", 0) or 0) >= config.CONVERSATION_THRESHOLD_SECONDS:
            pid = call.get("personId")
            if pid:
                candidate_ids.add(pid)

    if not candidate_ids:
        return set()

    print(f"  Looking up {len(candidate_ids)} lead sources...", flush=True)

    excluded = set()
    for pid in candidate_ids:
        try:
            person = client._request("GET", f"people/{pid}")
            source = (person.get("source") or "").lower()
            if source in excluded_lower:
                excluded.add(pid)
        except Exception:
            pass

    if excluded:
        print(f"  Excluding {len(excluded)} leads with source: "
              f"{', '.join(config.EXCLUDED_LEAD_SOURCES)}")

    return excluded


def calculate_speed_to_lead(client, user_id, since):
    """
    Calculate average speed-to-lead for newly routed leads only.

    Only measures leads that were auto-assigned to the agent via lead
    routing (createdVia = API or Email Parsing), NOT leads the agent
    picked up from the pond or created manually.

    Excludes Sphere leads (past clients, not purchased).
    Returns (avg_minutes, num_leads_measured) or (None, 0).
    """
    # Routed lead sources — these are new leads pushed to the agent
    ROUTED_VIA = {"api", "email parsing"}
    excluded_sources = {s.lower() for s in config.EXCLUDED_LEAD_SOURCES}

    people = client.get_people(
        assigned_user_id=user_id,
        created_since=since,
        limit=100
    )

    if not people:
        return None, 0

    speeds = []
    for person in people:
        # Only measure leads that were routed to the agent (not pond/manual)
        created_via = (person.get("createdVia") or "").lower()
        if created_via not in ROUTED_VIA:
            continue

        # Skip Sphere and other excluded sources
        source = (person.get("source") or "").lower()
        if source in excluded_sources:
            continue

        created_at = person.get("created")
        if not created_at or not isinstance(created_at, str):
            continue

        try:
            created_dt = datetime.fromisoformat(
                created_at.replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            continue

        first_contact = None
        for field in ["firstCallMade", "lastActivity", "firstOutboundCall"]:
            val = person.get(field)
            if val:
                try:
                    first_contact = datetime.fromisoformat(
                        val.replace("Z", "+00:00")
                    )
                    break
                except (ValueError, TypeError):
                    continue

        if first_contact and first_contact > created_dt:
            diff_minutes = (first_contact - created_dt).total_seconds() / 60
            # Only count reasonable values (< 24 hours)
            if diff_minutes < 1440:
                speeds.append(diff_minutes)

    if not speeds:
        return None, 0

    return sum(speeds) / len(speeds), len(speeds)


def count_compliance_violations(client, user_id, tag):
    """
    Count leads with the compliance violation tag assigned to this agent.
    FUB's tag filter is unreliable, so we fetch all leads and filter
    client-side by checking the tags array.
    Returns (total_count, leads_count, sphere_count).
    """
    people = client.get_people(
        assigned_user_id=user_id,
        limit=500
    )

    if not people:
        return 0, 0, 0

    tag_lower = tag.lower()
    sphere = 0
    leads = 0

    for person in people:
        tags = [t.lower() for t in (person.get("tags") or [])]
        if tag_lower not in tags:
            continue
        source = (person.get("source") or "").lower()
        if source == "sphere":
            sphere += 1
        else:
            leads += 1

    total = leads + sphere
    return total, leads, sphere


def count_appointments_for_user(all_appointments, user_id):
    """
    Count appointments set and met for an agent.
    Appt Set:  appointments where the agent is an invitee AND a lead (personId) is attached
    Appt Met:  subset where outcome == 'Met with Client'
    Returns (appts_set, appts_met).
    """
    appts_set = 0
    appts_met = 0

    for appt in all_appointments:
        invitees = appt.get("invitees", [])

        # Check if this agent is an invitee
        agent_involved = any(inv.get("userId") == user_id for inv in invitees)
        if not agent_involved:
            continue

        # Check if a lead (personId) is attached
        has_lead = any(inv.get("personId") for inv in invitees)
        if not has_lead:
            continue

        appts_set += 1
        if appt.get("outcome") == "Met with Client":
            appts_met += 1

    return appts_set, appts_met


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_agent(metrics):
    """
    Evaluate an agent against KPIs.
    Returns dict with pass/fail for each and overall status.
    """
    results = {
        "calls_pass": metrics["outbound_calls"] >= config.MIN_OUTBOUND_CALLS,
        "convos_pass": metrics["conversations"] >= config.MIN_CONVERSATIONS,
        "speed_pass": True,
        "compliance_pass": metrics["compliance_violations"] <= config.MAX_OUT_OF_COMPLIANCE,
    }

    if config.ENABLE_SPEED_TO_LEAD and metrics["speed_to_lead_avg"] is not None:
        results["speed_pass"] = (
            metrics["speed_to_lead_avg"] <= config.MAX_SPEED_TO_LEAD_MINUTES
        )

    kpis = [
        results["calls_pass"],
        results["convos_pass"],
        results["compliance_pass"],
    ]
    if config.ENABLE_SPEED_TO_LEAD:
        kpis.append(results["speed_pass"])

    results["overall_pass"] = all(kpis)
    return results


# ---------------------------------------------------------------------------
# Main audit
# ---------------------------------------------------------------------------

def run_audit(client, weeks_back=1):
    """Run the full KPI audit for all agents."""
    # Audit covers the 7 days ending yesterday (not including today)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    until = today  # midnight today = end of yesterday
    since = today - timedelta(days=config.AUDIT_PERIOD_DAYS * weeks_back)

    print("=" * 72)
    print("LEGACY HOME TEAM — WEEKLY KPI AUDIT")
    last_day = until - timedelta(days=1)
    print(f"Period: {since.strftime('%b %d')} — {last_day.strftime('%b %d, %Y')}")
    print(f"Thresholds: Calls ≥{config.MIN_OUTBOUND_CALLS}  "
          f"Convos ≥{config.MIN_CONVERSATIONS}  "
          f"OOC ≤{config.MAX_OUT_OF_COMPLIANCE}")
    print("=" * 72)
    print()

    # Auto-detect agents from FUB
    print("Auto-detecting agents from FUB...")
    agent_map = auto_detect_agents(client)

    if not agent_map:
        print("ERROR: No active agents found in FUB.")
        sys.exit(1)

    print(f"Found {len(agent_map)} active agents: "
          f"{', '.join(sorted(agent_map.keys()))}")
    if config.EXCLUDED_USERS:
        print(f"Excluded: {', '.join(config.EXCLUDED_USERS)}")
    print()

    # Fetch calls per-agent — avoids 2000-record cap being exhausted by system
    # accounts and post-window calls before the audit window is reached.
    print("  Fetching calls (per-agent)...", flush=True)
    all_calls = []
    seen_call_ids: set = set()
    for _aname, _auser in agent_map.items():
        for _c in client.get_calls(user_id=_auser["id"], since=since, until=until):
            _cid = _c.get("id")
            if _cid not in seen_call_ids:
                seen_call_ids.add(_cid)
                all_calls.append(_c)
    print(f"  Found {len(all_calls)} total calls")

    # Build excluded personIds (e.g., Sphere leads)
    excluded_person_ids = build_excluded_person_ids(client, all_calls)

    # Fetch all appointments once
    print("  Fetching appointments...", flush=True)
    all_appointments = client.get_appointments(since=since, until=until)
    print(f"  Found {len(all_appointments)} appointments")
    print()

    # Collect metrics for each agent
    all_results = {}

    for name, user in sorted(agent_map.items()):
        user_id = user["id"]
        print(f"  Checking {name}...", end=" ", flush=True)

        # KPI 1 & 2: Calls and conversations
        outbound, convos, talk_secs = count_calls_for_user(
            all_calls, user_id, excluded_person_ids
        )

        # KPI 3: Speed to lead
        speed_avg, speed_count = calculate_speed_to_lead(
            client, user_id, since
        )

        # KPI 4: Compliance (with breakdown)
        violations, ooc_leads, ooc_sphere = count_compliance_violations(
            client, user_id, config.COMPLIANCE_TAG
        )

        # Appointments
        appts_set, appts_met = count_appointments_for_user(
            all_appointments, user_id
        )

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

        all_results[name] = {
            "user": user,
            "metrics": metrics,
            "evaluation": evaluation,
        }

        status = "✅ PASS" if evaluation["overall_pass"] else "❌ FAIL"
        print(status)

    return all_results, since, until


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

def print_report(results):
    """Print the formatted KPI scorecard."""
    print()
    print("=" * 72)
    print("KPI SCORECARD")
    print("=" * 72)
    print()

    header = (
        f"{'Agent':<22} "
        f"{'Calls':>7} "
        f"{'Convos':>7} "
        f"{'STL':>8} "
        f"{'OOC':>5} "
        f"{'Status':>8}"
    )
    print(header)
    print(
        f"{'':.<22} "
        f"{'(≥' + str(config.MIN_OUTBOUND_CALLS) + ')':>7} "
        f"{'(≥' + str(config.MIN_CONVERSATIONS) + ')':>7} "
        f"{'(<' + str(config.MAX_SPEED_TO_LEAD_MINUTES) + 'm)':>8} "
        f"{'(≤' + str(config.MAX_OUT_OF_COMPLIANCE) + ')':>5} "
        f"{'':>8}"
    )
    print("-" * 72)

    passed = []
    failed = []

    for name, data in sorted(results.items()):
        m = data["metrics"]
        e = data["evaluation"]

        calls_str = f"{m['outbound_calls']}"
        if not e["calls_pass"]:
            calls_str += " ✗"

        convos_str = f"{m['conversations']}"
        if not e["convos_pass"]:
            convos_str += " ✗"

        if m["speed_to_lead_avg"] is not None:
            speed_str = f"{m['speed_to_lead_avg']:.1f}m"
            if not e["speed_pass"]:
                speed_str += " ✗"
        else:
            speed_str = "n/a"

        comp_str = f"{m['compliance_violations']}"
        if not e["compliance_pass"]:
            comp_str += " ✗"

        status = "PASS" if e["overall_pass"] else "FAIL"

        print(
            f"{name:<22} "
            f"{calls_str:>7} "
            f"{convos_str:>7} "
            f"{speed_str:>8} "
            f"{comp_str:>5} "
            f"{status:>8}"
        )

        if e["overall_pass"]:
            passed.append(name)
        else:
            failed.append(name)

    # Summary
    print()
    print("-" * 72)
    print(f"PRIORITY GROUP THIS WEEK: {len(passed)} agent(s)")
    if passed:
        for name in passed:
            print(f"  ✅ {name}")
    else:
        print("  (nobody qualified — all leads go to pond)")

    if failed:
        print(f"\nPOND ONLY: {len(failed)} agent(s)")
        for name in failed:
            print(f"  ❌ {name}")

    return passed, failed


def print_conversion_funnel(results):
    """Print the conversion funnel: Calls → Convos → Appts Set → Appts Met."""
    print()
    print("=" * 72)
    print("CONVERSION FUNNEL")
    print("=" * 72)
    print()

    header = (
        f"{'Agent':<22} "
        f"{'Calls':>7} "
        f"{'Convos':>7} "
        f"{'ApptSet':>8} "
        f"{'ApptMet':>8} "
        f"{'Conv %':>7}"
    )
    print(header)
    print("-" * 72)

    for name, data in sorted(results.items()):
        m = data["metrics"]
        calls = m["outbound_calls"]
        convos = m["conversations"]
        appts_set = m["appts_set"]
        appts_met = m["appts_met"]

        # Conversion rate: conversations / calls
        if calls > 0:
            conv_pct = f"{(convos / calls * 100):.0f}%"
        else:
            conv_pct = "—"

        print(
            f"{name:<22} "
            f"{calls:>7} "
            f"{convos:>7} "
            f"{appts_set:>8} "
            f"{appts_met:>8} "
            f"{conv_pct:>7}"
        )

    print()


def print_ooc_breakdown(results):
    """Print the out-of-compliance breakdown by lead source."""
    print("=" * 72)
    print("OUT OF COMPLIANCE BREAKDOWN")
    print("=" * 72)
    print()

    any_ooc = False
    for name, data in sorted(results.items()):
        m = data["metrics"]
        total = m["compliance_violations"]
        if total == 0:
            continue
        any_ooc = True
        leads = m["ooc_leads"]
        sphere = m["ooc_sphere"]
        print(f"  {name}: {total} total OOC")
        print(f"    → {leads} Leads (purchased)")
        print(f"    → {sphere} Sphere (past clients)")
        print()

    if not any_ooc:
        print("  All agents are in compliance! 🎉")
        print()


def print_live_calls_report(results):
    """
    Print a report of who should be in the Live Calls shared inbox.
    The API does not support managing shared inboxes (403), so this
    report tells the admin who to add/remove manually.
    """
    if config.LIVE_CALLS_INBOX_ID is None:
        return

    passed = [name for name, d in results.items() if d["evaluation"]["overall_pass"]]
    failed = [name for name, d in results.items() if not d["evaluation"]["overall_pass"]]

    admin = getattr(config, "LIVE_CALLS_ADMIN", "Admin")

    print("=" * 72)
    print(f"LIVE CALLS INBOX — @{admin} Action Required")
    print("(API cannot manage shared inboxes automatically)")
    print("=" * 72)
    print()

    if passed:
        print("  ADD to Live Calls inbox:")
        for name in sorted(passed):
            print(f"    ✅ {name}")
    else:
        print("  ADD to Live Calls inbox: (none)")

    if failed:
        print()
        print("  REMOVE from Live Calls inbox:")
        for name in sorted(failed):
            print(f"    ❌ {name}")

    print()


# ---------------------------------------------------------------------------
# Group management
# ---------------------------------------------------------------------------

def update_priority_group(client, results, passed_agents):
    """Update the Priority Agents group in FUB."""
    if config.PRIORITY_GROUP_ID is None:
        print("\n⚠  PRIORITY_GROUP_ID not set in config.py")
        print("   Run 'python kpi_audit.py --list-groups' to find your group ID.")
        return False

    user_ids = []
    for name in passed_agents:
        user_id = results[name]["user"]["id"]
        user_ids.append(user_id)

    # Add protected agents
    if config.PROTECTED_AGENTS:
        for pname in config.PROTECTED_AGENTS:
            puser = client.get_user_by_name(pname)
            if puser and puser["id"] not in user_ids:
                user_ids.append(puser["id"])
                print(f"  ➕ {pname} (protected — always in group)")

    print(f"\nUpdating Priority Group (ID: {config.PRIORITY_GROUP_ID})...")
    print(f"  Setting {len(user_ids)} agent(s) in group")

    try:
        client.update_group(config.PRIORITY_GROUP_ID, user_ids)
        print("  ✅ Group updated successfully!")
        return True
    except Exception as e:
        print(f"  ❌ Failed to update group: {e}")
        return False


# ---------------------------------------------------------------------------
# Utility commands
# ---------------------------------------------------------------------------

def list_groups(client):
    """List all groups in FUB."""
    groups = client.get_groups()
    print("\nFUB GROUPS:")
    print("-" * 50)
    if not groups:
        print("  No groups found.")
        return
    for g in groups:
        gid = g.get("id", "?")
        gname = g.get("name", "unnamed")
        gtype = g.get("type", "?")
        users = g.get("userIds", [])
        print(f"  ID: {gid:>4}  |  {gname:<30}  |  Type: {gtype}  |  {len(users)} members")


def list_users(client):
    """List all users in FUB."""
    users = client.get_users()
    print("\nFUB USERS:")
    print("-" * 50)
    for u in users:
        uid = u.get("id", "?")
        fname = u.get("firstName", "")
        lname = u.get("lastName", "")
        role = u.get("role", "")
        status = u.get("status", "")
        print(f"  ID: {uid:>4}  |  {fname} {lname:<20}  |  {role:<12}  |  {status}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    # Apply CLI threshold overrides
    apply_threshold_overrides(args)

    try:
        client = FUBClient(api_key=args.api_key)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Utility modes
    if args.list_groups:
        list_groups(client)
        print(f"\n({client.request_count} API requests made)")
        return

    if args.list_users:
        list_users(client)
        print(f"\n({client.request_count} API requests made)")
        return

    # Run the audit
    results, period_start, period_end = run_audit(client, weeks_back=args.weeks_back)
    passed, failed = print_report(results)

    # Conversion funnel
    print_conversion_funnel(results)

    # OOC breakdown
    print_ooc_breakdown(results)

    # Live Calls inbox report
    print_live_calls_report(results)

    # Optionally update the group
    if args.update_group:
        update_priority_group(client, results, passed)
    else:
        print("💡 Run with --update-group to automatically update FUB routing.")

    # Email report
    if args.email:
        send_report(results, period_start, period_end)

    print(f"\n({client.request_count} API requests made)")
    print(f"Audit complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
