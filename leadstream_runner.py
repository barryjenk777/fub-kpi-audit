"""
LeadStream Runner — Scheduled execution of lead priority scoring.

Run this via cron, Railway scheduled task, or Claude scheduled tasks.

Schedules:
  - Full run (agents + pond): every 4 hours (6am, 10am, 2pm, 6pm)
  - Pond-only refresh: every hour (suppresses contacted leads faster)

Usage:
    python leadstream_runner.py              # Full live run (agents + pond)
    python leadstream_runner.py --pond-only  # Pond refresh only
    python leadstream_runner.py --dry-run    # Preview only
"""

import argparse
import sys
from datetime import datetime, timezone

from lead_scoring import LeadScorer, _get_leadstream_client


def main():
    parser = argparse.ArgumentParser(description="LeadStream Scheduled Runner")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview scoring without applying tags")
    parser.add_argument("--pond-only", action="store_true",
                        help="Only refresh pond leads (faster, for hourly runs)")
    args = parser.parse_args()

    dry_run = args.dry_run
    mode = "POND-ONLY" if args.pond_only else "FULL"

    print(f"LeadStream Runner [{mode}] started at "
          f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    try:
        client = _get_leadstream_client()
        scorer = LeadScorer(client)
        results = scorer.run(dry_run=dry_run, pond_only=args.pond_only)

        # Summary for logging
        agent_count = len(results.get("agents", {}))
        total_agent_leads = sum(r["count"] for r in results["agents"].values())
        pond_count = len(results.get("pond", []))

        print(f"\nLeadStream complete: {total_agent_leads} agent leads + "
              f"{pond_count} pond leads across {agent_count} agents")
        print(f"Total API requests: {client.request_count}")

    except Exception as e:
        print(f"\nLeadStream ERROR: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
