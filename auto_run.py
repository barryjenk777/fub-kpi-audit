#!/usr/bin/env python3
"""
Automated weekly KPI audit run.
Reads saved settings, runs audit, updates group, sends email.
Designed to be triggered by a scheduler (cron, Claude scheduled task, etc.)
"""

import os
import sys
import json

# Ensure we're in the right directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

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
    run_audit,
    print_report,
    print_conversion_funnel,
    print_ooc_breakdown,
    print_live_calls_report,
    update_priority_group,
)
from email_report import send_report
from datetime import datetime, timedelta, timezone


def load_saved_settings():
    """Load and apply saved KPI thresholds."""
    settings_file = os.path.join(os.path.dirname(__file__), "settings.json")
    try:
        with open(settings_file) as f:
            s = json.load(f)
        if "min_calls" in s:
            config.MIN_OUTBOUND_CALLS = s["min_calls"]
        if "min_convos" in s:
            config.MIN_CONVERSATIONS = s["min_convos"]
        if "max_ooc" in s:
            config.MAX_OUT_OF_COMPLIANCE = s["max_ooc"]
        print(f"Loaded settings: Calls ≥{config.MIN_OUTBOUND_CALLS}, "
              f"Convos ≥{config.MIN_CONVERSATIONS}, "
              f"OOC ≤{config.MAX_OUT_OF_COMPLIANCE}")
    except FileNotFoundError:
        print("No saved settings found, using config.py defaults")


def main():
    print("=" * 60)
    print("AUTOMATED WEEKLY KPI AUDIT")
    print(f"Run time: {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}")
    print("=" * 60)
    print()

    # Load saved settings
    load_saved_settings()

    # Init client
    client = FUBClient()

    # Run audit
    results, period_start, period_end = run_audit(client)
    passed, failed = print_report(results)

    # Print all reports
    print_conversion_funnel(results)
    print_ooc_breakdown(results)
    print_live_calls_report(results)

    # Update Priority Agents group
    print()
    update_priority_group(client, results, passed)

    # Send email
    print()
    send_report(results, period_start, period_end)

    print(f"\n({client.request_count} API requests made)")
    print(f"Auto-run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
