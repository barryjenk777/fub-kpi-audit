"""
LeadStream — Daily Lead Priority Scoring Engine

Scores leads based on Ylopo engagement signals, IDX site visit recency,
and contact history. Tags the top leads per agent so they can use a FUB
smart list as their daily call priority list.

Usage:
    python lead_scoring.py                  # Dry run (no tagging)
    python lead_scoring.py --apply          # Score + tag leads in FUB
    python lead_scoring.py --agent "Name"   # Score one agent only
    python lead_scoring.py --pond-only      # Score pond leads only
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from config import (
    EXCLUDED_USERS,
    LEADSTREAM_AGING_NEW_POINTS,
    LEADSTREAM_API_KEY_ENV,
    LEADSTREAM_EXCLUDED_SOURCES,
    LEADSTREAM_LIMIT,
    LEADSTREAM_MULTI_SIGNAL_BONUS,
    LEADSTREAM_NEW_LEAD_24H_BONUS,
    LEADSTREAM_NEW_LEAD_72H_BONUS,
    LEADSTREAM_POND_LIMIT,
    LEADSTREAM_POND_TAG,
    LEADSTREAM_REENGAGE_MAX_DAYS,
    LEADSTREAM_REENGAGE_MIN_DAYS,
    LEADSTREAM_COLD_LEAD_POINTS,
    LEADSTREAM_REENGAGE_POINTS,
    LEADSTREAM_SELLER_BONUS,
    LEADSTREAM_SIGNAL_TAGS,
    LEADSTREAM_STALE_DAYS,
    LEADSTREAM_STALE_HOT_POINTS,
    LEADSTREAM_SUPPRESS_HOURS,
    LEADSTREAM_TAG,
    LEADSTREAM_VISIT_RECENCY,
    SELLER_TAGS,
)
from fub_client import FUBClient


def parse_dt(value):
    """Parse an ISO 8601 datetime string to a timezone-aware datetime."""
    if not value:
        return None
    # Handle Z suffix and various ISO formats
    value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def hours_ago(dt, now):
    """Return how many hours ago a datetime was, or None if dt is missing."""
    if not dt:
        return None
    delta = now - dt
    return delta.total_seconds() / 3600


class LeadScorer:
    """Scores and prioritizes leads for agents based on engagement signals."""

    def __init__(self, client):
        self.client = client
        self.now = datetime.now(timezone.utc)
        # Pre-built map of personId → most recent site visit datetime
        self._visit_map = None

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score_lead(self, person, agent_calls=None, agent_texts=None):
        """
        Score a single lead. Returns (score, tier_label, breakdown).

        agent_calls: list of recent calls for this agent (pre-fetched)
        agent_texts: list of recent texts for this agent (pre-fetched)
        """
        person_id = person.get("id")
        tags = person.get("tags") or []
        score = 0
        tier = "NONE"
        breakdown = []

        # --- 0. Source exclusion (e.g., Courted.io) ---
        source = person.get("source", "")
        if source in LEADSTREAM_EXCLUDED_SOURCES:
            return 0, "EXCLUDED_SOURCE", [f"source '{source}' excluded"]

        # --- 1. Ylopo signal tags (use highest, add multi-signal bonus) ---
        signal_scores = []
        for tag, points in LEADSTREAM_SIGNAL_TAGS.items():
            if tag in tags:
                signal_scores.append((tag, points))

        if signal_scores:
            # Use highest signal
            best_tag, best_points = max(signal_scores, key=lambda x: x[1])
            score += best_points
            tier = best_tag
            breakdown.append(f"{best_tag}: +{best_points}")

            # Multi-signal bonus
            if len(signal_scores) > 1:
                score += LEADSTREAM_MULTI_SIGNAL_BONUS
                other_tags = [t for t, _ in signal_scores if t != best_tag]
                breakdown.append(f"multi-signal ({', '.join(other_tags)}): +{LEADSTREAM_MULTI_SIGNAL_BONUS}")

        # --- 2. IDX site visit recency (from Events API) ---
        last_visit = self._get_last_visit(person.get("id"))
        visit_hours = hours_ago(last_visit, self.now)
        if visit_hours is not None:
            for threshold_hours, points in LEADSTREAM_VISIT_RECENCY:
                if visit_hours <= threshold_hours:
                    score += points
                    breakdown.append(f"site visit {visit_hours:.0f}h ago: +{points}")
                    if not signal_scores:
                        tier = "SITE_ACTIVE"
                    break

        # --- 3. Stale hot: had signal tag + no agent contact in 3+ days ---
        if not signal_scores:
            last_contact_hours = self._hours_since_last_contact(
                person_id, agent_calls, agent_texts
            )
            created = parse_dt(person.get("created"))
            created_hours = hours_ago(created, self.now) if created else None

            if last_contact_hours is not None:
                stale_threshold = LEADSTREAM_STALE_DAYS * 24
                reengage_min = LEADSTREAM_REENGAGE_MIN_DAYS * 24
                reengage_max = LEADSTREAM_REENGAGE_MAX_DAYS * 24

                if last_contact_hours >= stale_threshold and last_contact_hours < reengage_min:
                    score += LEADSTREAM_STALE_HOT_POINTS
                    tier = "STALE_HOT"
                    breakdown.append(f"no contact in {last_contact_hours / 24:.0f}d: +{LEADSTREAM_STALE_HOT_POINTS}")
                elif reengage_min <= last_contact_hours <= reengage_max:
                    score += LEADSTREAM_REENGAGE_POINTS
                    tier = "RE_ENGAGE"
                    breakdown.append(f"last contact {last_contact_hours / 24:.0f}d ago: +{LEADSTREAM_REENGAGE_POINTS}")
            elif created_hours is not None and created_hours <= 168:
                # New lead, never contacted
                score += LEADSTREAM_AGING_NEW_POINTS
                tier = "AGING_NEW"
                breakdown.append(f"new lead ({created_hours / 24:.0f}d old), never contacted: +{LEADSTREAM_AGING_NEW_POINTS}")
            else:
                # Cold lead: older than 7 days, no contact in lookback window
                score += LEADSTREAM_COLD_LEAD_POINTS
                tier = "COLD_LEAD"
                breakdown.append(f"cold lead (no contact in {LEADSTREAM_REENGAGE_MAX_DAYS}d+): +{LEADSTREAM_COLD_LEAD_POINTS}")

        # --- 4. New lead recency boost ---
        created = parse_dt(person.get("created"))
        if created:
            age_hours = hours_ago(created, self.now)
            if age_hours is not None:
                if age_hours <= 24:
                    score += LEADSTREAM_NEW_LEAD_24H_BONUS
                    breakdown.append(f"created {age_hours:.0f}h ago: +{LEADSTREAM_NEW_LEAD_24H_BONUS}")
                elif age_hours <= 72:
                    score += LEADSTREAM_NEW_LEAD_72H_BONUS
                    breakdown.append(f"created {age_hours / 24:.0f}d ago: +{LEADSTREAM_NEW_LEAD_72H_BONUS}")

        # --- 5. Seller bonus ---
        if any(t in tags for t in SELLER_TAGS):
            score += LEADSTREAM_SELLER_BONUS
            breakdown.append(f"seller lead: +{LEADSTREAM_SELLER_BONUS}")

        # --- 6. Suppression check ---
        suppress_hours = LEADSTREAM_SUPPRESS_HOURS
        last_attempt_hours = self._hours_since_last_attempt(
            person_id, agent_calls, agent_texts
        )
        if last_attempt_hours is not None and last_attempt_hours < suppress_hours:
            # Check if there was a real conversation (2+ min call)
            had_conversation = self._had_recent_conversation(
                person_id, agent_calls, suppress_hours
            )
            if not had_conversation:
                # Attempted but no conversation — suppress
                breakdown.append(f"attempted {last_attempt_hours:.0f}h ago, suppressed")
                return 0, "SUPPRESSED", breakdown

        return score, tier, breakdown

    def _build_visit_map(self):
        """Fetch recent IDX events and build a map of personId → latest visit time.

        Queries the Events API for 'Viewed Page' and 'Viewed Property' events
        from the last 7 days, then keeps only the most recent per person.
        """
        if self._visit_map is not None:
            return  # already built

        self._visit_map = {}
        # Only look back 3 days for site visits — enough for the scoring tiers
        # and keeps API calls manageable (max 5 pages × 3 event types = 15 requests)
        since = self.now - timedelta(days=3)

        for event_type in ["Viewed Page", "Viewed Property", "Property Saved"]:
            try:
                events = self.client.get_events(
                    since=since, event_type=event_type, max_pages=5
                )
                for event in events:
                    pid = event.get("personId")
                    if not pid:
                        continue
                    dt = parse_dt(event.get("created") or event.get("occurred"))
                    if dt and (pid not in self._visit_map or dt > self._visit_map[pid]):
                        self._visit_map[pid] = dt
            except Exception:
                # Events API may not be available; continue without it
                pass

    def _get_last_visit(self, person_id):
        """Get the most recent site visit datetime for a person."""
        self._build_visit_map()
        return self._visit_map.get(person_id)

    def _hours_since_last_contact(self, person_id, calls, texts):
        """Hours since last outbound call or text to this person."""
        latest = None

        if calls:
            for call in calls:
                if call.get("personId") == person_id and not call.get("isIncoming"):
                    dt = parse_dt(call.get("created"))
                    if dt and (latest is None or dt > latest):
                        latest = dt

        if texts:
            for text in texts:
                if text.get("personId") == person_id and text.get("isOutbound"):
                    dt = parse_dt(text.get("created"))
                    if dt and (latest is None or dt > latest):
                        latest = dt

        return hours_ago(latest, self.now)

    def _hours_since_last_attempt(self, person_id, calls, texts):
        """Hours since last outbound attempt (call or text) to this person."""
        # Same as _hours_since_last_contact for now
        return self._hours_since_last_contact(person_id, calls, texts)

    def _had_recent_conversation(self, person_id, calls, within_hours):
        """Check if there was a 2+ min call with this person recently."""
        if not calls:
            return False
        cutoff = self.now - timedelta(hours=within_hours)
        for call in calls:
            if call.get("personId") == person_id:
                dt = parse_dt(call.get("created"))
                duration = call.get("duration", 0)
                if dt and dt >= cutoff and duration >= 120:
                    return True
        return False

    # ------------------------------------------------------------------
    # Agent lead scoring
    # ------------------------------------------------------------------

    def score_agent_leads(self, agent_id, agent_name=None, limit=None):
        """
        Score all leads assigned to an agent. Returns sorted list of
        (person, score, tier, breakdown) tuples.
        """
        if limit is None:
            limit = LEADSTREAM_LIMIT

        # Fetch agent's assigned leads
        people = self.client.get_people(assigned_user_id=agent_id)
        if not people:
            return []

        # Fetch recent calls and texts for contact history
        since = self.now - timedelta(days=LEADSTREAM_REENGAGE_MAX_DAYS)
        calls = self.client.get_calls(user_id=agent_id, since=since)
        texts = self._get_agent_texts_by_person(agent_id, since)

        # Score each lead
        scored = []
        for person in people:
            score, tier, breakdown = self.score_lead(person, calls, texts)
            if score > 0:
                scored.append((person, score, tier, breakdown))

        # Sort by score descending, take top N
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:limit]

    def _get_agent_texts_by_person(self, agent_id, since):
        """Fetch recent text messages for an agent. Returns list of text dicts."""
        try:
            return self.client.get_text_messages(user_id=agent_id, since=since)
        except Exception:
            # Text message API can be flaky; don't block scoring
            return []

    # ------------------------------------------------------------------
    # Pond lead scoring
    # ------------------------------------------------------------------

    @staticmethod
    def _is_pond_lead(person):
        """Check if a lead is in the pond (shark tank).

        FUB pond leads have an assignedPondId set. They may still have an
        assignedUserId (e.g., ISA), but the pondId indicates they're in the
        shared pool waiting to be claimed by an agent.
        """
        return bool(person.get("assignedPondId"))

    def score_pond_leads(self, limit=None):
        """
        Score pond (shark tank) leads. Pond leads have an assignedPondId.
        Uses site visits, signal tags, and lead recency to prioritize.
        Suppresses leads that any agent has already called/texted recently.
        Returns sorted list of (person, score, tier, breakdown) tuples.
        """
        if limit is None:
            limit = LEADSTREAM_POND_LIMIT

        scored = []
        seen_ids = set()

        # Build visit map first so we know who's been on the site
        self._build_visit_map()

        # Build set of personIds contacted by ANY agent in the last 2 hours
        # so we can suppress pond leads that have already been worked
        contacted_ids = self._get_recently_contacted_pond_ids()

        # Fetch recently updated leads and filter to pond leads client-side
        since = self.now - timedelta(days=7)
        recent_people = self.client.get_people(updated_since=since)

        for person in recent_people:
            pid = person.get("id")
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            # Only include pond leads
            if not self._is_pond_lead(person):
                continue

            # Suppress if any agent already contacted this lead recently
            if pid in contacted_ids:
                continue

            score, tier, breakdown = self.score_lead(person)
            if score > 0:
                scored.append((person, score, tier, breakdown))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:limit]

    def _get_recently_contacted_pond_ids(self):
        """Get personIds of pond leads contacted by ANY agent in the last 2 hours.

        Checks recent outbound calls and texts across all agents so that
        once one agent works a pond lead, it drops off the list for everyone.
        """
        contacted = set()
        since = self.now - timedelta(hours=2)

        # Check recent outbound calls (all agents)
        try:
            calls = self.client.get_calls(since=since)
            for call in calls:
                if not call.get("isIncoming"):
                    pid = call.get("personId")
                    if pid:
                        contacted.add(pid)
        except Exception:
            pass

        # Check recent outbound texts (all agents)
        try:
            texts = self.client.get_text_messages(since=since)
            for text in texts:
                if text.get("isOutbound"):
                    pid = text.get("personId")
                    if pid:
                        contacted.add(pid)
        except Exception:
            pass

        return contacted

    # ------------------------------------------------------------------
    # Tag management
    # ------------------------------------------------------------------

    # Use /tmp on Railway (app dir is read-only), local .cache otherwise
    _cache_dir = "/tmp/.cache" if os.path.exists("/tmp") and not os.access(os.path.dirname(__file__), os.W_OK) else os.path.join(os.path.dirname(__file__), ".cache")
    MANIFEST_FILE = os.path.join(_cache_dir, "leadstream_manifest.json")

    def _load_manifest(self):
        """Load the manifest of previously tagged lead IDs."""
        try:
            with open(self.MANIFEST_FILE) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"agent": {}, "pond": []}

    def _save_manifest(self, manifest):
        """Save the manifest of tagged lead IDs."""
        os.makedirs(os.path.dirname(self.MANIFEST_FILE), exist_ok=True)
        try:
            with open(self.MANIFEST_FILE, "w") as f:
                json.dump(manifest, f, indent=2)
        except OSError:
            # Read-only filesystem (e.g., Railway) — skip
            pass

    def cleanup_tags(self, dry_run=True):
        """Remove LeadStream tags from previously tagged leads using the manifest."""
        manifest = self._load_manifest()
        removed = 0

        # Clean agent leads
        for agent_name, lead_items in manifest.get("agent", {}).items():
            for item in lead_items:
                pid = item["id"] if isinstance(item, dict) else item
                if dry_run:
                    print(f"  [DRY RUN] Would remove '{LEADSTREAM_TAG}' from ID: {pid} ({agent_name})")
                else:
                    try:
                        person = self.client.get_person(pid)
                        tags = person.get("tags") or []
                        self.client.remove_tag_fast(pid, LEADSTREAM_TAG, tags)
                    except Exception:
                        pass
                removed += 1

        # Clean pond leads
        for item in manifest.get("pond", []):
            pid = item["id"] if isinstance(item, dict) else item
            if dry_run:
                print(f"  [DRY RUN] Would remove '{LEADSTREAM_POND_TAG}' from ID: {pid} (pond)")
            else:
                try:
                    person = self.client.get_person(pid)
                    tags = person.get("tags") or []
                    self.client.remove_tag_fast(pid, LEADSTREAM_POND_TAG, tags)
                except Exception:
                    pass
            removed += 1

        return removed

    def apply_tags(self, scored_leads, tag, dry_run=True):
        """Apply a tag to the scored leads. Returns (count, list of tagged IDs)."""
        tagged = 0
        tagged_ids = []
        for person, score, tier, breakdown in scored_leads:
            pid = person.get("id")
            name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
            existing_tags = person.get("tags") or []

            if dry_run:
                print(f"  [DRY RUN] Would tag {name} (ID: {pid}) with '{tag}' "
                      f"[score={score}, tier={tier}]")
            else:
                self.client.add_tag_fast(pid, tag, existing_tags)
            tagged += 1
            tagged_ids.append(pid)

        return tagged, tagged_ids

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run(self, dry_run=True, agent_name=None, pond_only=False):
        """
        Run the full LeadStream pipeline:
        1. Clean up old tags
        2. Score assigned leads per agent
        3. Score pond leads
        4. Apply new tags
        """
        print(f"\n{'='*60}")
        print(f"  LeadStream — Lead Priority Scoring")
        print(f"  {'DRY RUN' if dry_run else 'LIVE RUN'} at {self.now.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*60}\n")

        # Step 1: Cleanup
        print("Step 1: Cleaning up old LeadStream tags...")
        removed = self.cleanup_tags(dry_run=dry_run)
        print(f"  Removed tags from {removed} leads\n")

        results = {"removed": removed, "agents": {}, "pond": []}

        # Seed manifest from existing so pond-only runs don't wipe agent section
        existing_manifest = self._load_manifest()
        new_manifest = {
            "agent": existing_manifest.get("agent", {}),
            "pond": existing_manifest.get("pond", []),
        }

        # Step 2: Score assigned leads (skip if pond-only)
        if not pond_only:
            print("Step 2: Scoring assigned leads per agent...")
            agents = self._get_active_agents(agent_name)

            for agent in agents:
                agent_id = agent["id"]
                name = f"{agent.get('firstName', '')} {agent.get('lastName', '')}".strip()
                print(f"\n  Agent: {name}")
                print(f"  {'-'*40}")

                scored = self.score_agent_leads(agent_id, name)

                if not scored:
                    print("    No priority leads found")
                    continue

                for person, score, tier, breakdown in scored:
                    lead_name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
                    print(f"    {score:>3}pts [{tier:>20}] {lead_name}")
                    if breakdown:
                        print(f"         {' | '.join(breakdown)}")

                tagged, tagged_ids = self.apply_tags(scored, LEADSTREAM_TAG, dry_run=dry_run)
                print(f"  Tagged {tagged} leads for {name}")
                new_manifest["agent"][name] = [
                    {
                        "id": p.get("id"),
                        "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                        "score": s,
                        "tier": t,
                        "stage": p.get("stage", ""),
                    }
                    for p, s, t, _ in scored
                ]

                results["agents"][name] = {
                    "count": len(scored),
                    "leads": [
                        {
                            "id": p.get("id"),
                            "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                            "score": s,
                            "tier": t,
                            "breakdown": b,
                        }
                        for p, s, t, b in scored
                    ],
                }

        # Step 3: Score pond leads
        print(f"\n{'Step 3' if not pond_only else 'Step 2'}: Scoring pond leads...")
        pond_scored = self.score_pond_leads()

        if not pond_scored:
            print("  No hot pond leads found")
        else:
            for person, score, tier, breakdown in pond_scored:
                lead_name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
                print(f"    {score:>3}pts [{tier:>20}] {lead_name}")
                if breakdown:
                    print(f"         {' | '.join(breakdown)}")

            tagged, pond_ids = self.apply_tags(pond_scored, LEADSTREAM_POND_TAG, dry_run=dry_run)
            print(f"  Tagged {tagged} pond leads")
            new_manifest["pond"] = [
                {
                    "id": p.get("id"),
                    "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                    "score": s,
                    "tier": t,
                    "stage": p.get("stage", ""),
                }
                for p, s, t, _ in pond_scored
            ]

            results["pond"] = [
                {
                    "id": p.get("id"),
                    "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                    "score": s,
                    "tier": t,
                    "breakdown": b,
                }
                for p, s, t, b in pond_scored
            ]

        # Add run metadata
        new_manifest["last_run"] = self.now.isoformat()
        new_manifest["last_run_mode"] = "pond_only" if pond_only else "full"
        history = existing_manifest.get("run_history", [])
        agent_lead_count = sum(len(v) for v in new_manifest["agent"].values())
        pond_lead_count = len(new_manifest["pond"])
        history.append({
            "time": self.now.isoformat(),
            "mode": "pond_only" if pond_only else "full",
            "agent_leads": agent_lead_count,
            "pond_leads": pond_lead_count,
            "total": agent_lead_count + pond_lead_count,
        })
        new_manifest["run_history"] = history[-14:]

        # Save manifest for next cleanup cycle
        if not dry_run:
            self._save_manifest(new_manifest)

        # Summary
        total_tagged = sum(r["count"] for r in results["agents"].values()) + len(results["pond"])
        print(f"\n{'='*60}")
        print(f"  Summary: {total_tagged} leads tagged across "
              f"{len(results['agents'])} agents + pond")
        print(f"  API requests: {self.client.request_count}")
        print(f"{'='*60}\n")

        return results

    def _get_active_agents(self, agent_name=None):
        """Get active agents, optionally filtered by name."""
        users = self.client.get_users()
        agents = []
        for user in users:
            name = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
            if (user.get("status") or "").lower() != "active":
                continue
            if name in EXCLUDED_USERS:
                continue
            if agent_name and name.lower() != agent_name.lower():
                continue
            agents.append(user)
        return agents


# ======================================================================
# CLI
# ======================================================================

def _get_leadstream_client():
    """Create a FUBClient using the LeadStream API key if available, else default."""
    import os

    # Auto-load .env if present so the CLI works without manually sourcing it
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

    api_key = os.environ.get(LEADSTREAM_API_KEY_ENV)
    if api_key:
        print(f"  Using separate API key ({LEADSTREAM_API_KEY_ENV})")
        return FUBClient(api_key=api_key)
    return FUBClient()


def main():
    parser = argparse.ArgumentParser(description="LeadStream: Daily Lead Priority Scoring")
    parser.add_argument("--apply", action="store_true",
                        help="Apply tags to FUB (default is dry run)")
    parser.add_argument("--agent", type=str, default=None,
                        help="Score only a specific agent by name")
    parser.add_argument("--pond-only", action="store_true",
                        help="Score only pond leads")
    args = parser.parse_args()

    client = _get_leadstream_client()
    scorer = LeadScorer(client)
    scorer.run(dry_run=not args.apply, agent_name=args.agent, pond_only=args.pond_only)


if __name__ == "__main__":
    main()
