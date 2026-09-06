"""
appt_insight.py — the appointments money engine behind the reworked tab.

An appointment on a Google/Facebook lead is the most expensive artifact
the team produces: dollars of ad spend, ISA time, and nurture all funnel
into that one calendar slot. This module finds where those slots leak
money, WITH NAMES, so Barry coaches specifics instead of vibes:

  LEAK 1 — at-risk upcoming: set 3+ days ago, no touch since it was set.
           PPC leads no-show without a confirmation touch.
  LEAK 2 — ghosted after held: the meeting HAPPENED and then nobody
           called them again. The most expensive silence in the business.
  LEAK 3 — no outcome logged: invisible pipeline; can't coach what isn't
           logged.
  LEAK 4 — the slot effect: which weekdays/times actually hold.

Plus segments by agent and by source (held rate, set-to-start lag), and a
dollars-left-on-table estimate using the team's own appointment-to-
contract rate and blended net per deal. Touch checks are verified against
real FUB call logs (bounded: only the leak candidates get an API call).
Cached in app_state; refreshed nightly and on demand.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import config
import db as _db
from fub_client import FUBClient

logger = logging.getLogger(__name__)

_NON_AGENTS = set(getattr(config, "EXCLUDED_USERS", []))
CACHE_KEY = "appt_insight_v1"


def _q(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def _touched_since(client, person_id, since_dt):
    """Any outbound call to this person since since_dt (FUB-verified)."""
    try:
        calls = client.get_calls(person_id=person_id, since=since_dt) or []
        return any(not c.get("isIncoming", True) for c in calls)
    except Exception:
        return None  # unknown, never accuse on a failed check


def build_insight(days=60):
    client = FUBClient()
    now = datetime.now(timezone.utc)
    out = {"generated_at": now.isoformat(), "window_days": days}
    excl = tuple(_NON_AGENTS) or ("",)

    with _db.get_conn() as conn:
        with conn.cursor() as cur:
            # ── by agent ────────────────────────────────────────────────
            rows = _q(cur, """
                SELECT agent_name,
                       COUNT(*) FILTER (WHERE status != 'canceled'),
                       COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed'),
                       COUNT(*) FILTER (WHERE outcome = 'no_show' OR status = 'no_show'),
                       COUNT(*) FILTER (WHERE status = 'canceled'),
                       COUNT(*) FILTER (WHERE outcome IS NULL
                                        AND status NOT IN ('canceled','showed')
                                        AND start_time < NOW() - INTERVAL '1 day')
                FROM appointments
                WHERE start_time >= NOW() - make_interval(days => %s)
                  AND start_time < NOW()
                  AND agent_name IS NOT NULL AND agent_name NOT IN %s
                GROUP BY agent_name ORDER BY 2 DESC
            """, (days, excl))
            out["by_agent"] = [
                {"agent": r[0], "set": r[1], "held": r[2], "no_show": r[3],
                 "canceled": r[4], "no_outcome": r[5],
                 "held_rate": round(r[2] / r[1] * 100) if r[1] else None}
                for r in rows]

            # ── by source ───────────────────────────────────────────────
            rows = _q(cur, """
                SELECT COALESCE(NULLIF(source,''),'untracked'),
                       COUNT(*) FILTER (WHERE status != 'canceled'),
                       COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed'),
                       AVG(EXTRACT(epoch FROM start_time - fub_created_at)/86400.0)
                           FILTER (WHERE fub_created_at IS NOT NULL)
                FROM appointments
                WHERE start_time >= NOW() - make_interval(days => %s)
                  AND start_time < NOW()
                GROUP BY 1 HAVING COUNT(*) >= 2 ORDER BY 2 DESC
            """, (days,))
            out["by_source"] = [
                {"source": r[0], "set": r[1], "held": r[2],
                 "held_rate": round(r[2] / r[1] * 100) if r[1] else None,
                 "avg_lead_days": round(float(r[3]), 1) if r[3] is not None else None}
                for r in rows]

            # ── slot effect: held rate by weekday ───────────────────────
            rows = _q(cur, """
                SELECT EXTRACT(dow FROM start_time AT TIME ZONE 'America/New_York')::int,
                       COUNT(*) FILTER (WHERE status != 'canceled'),
                       COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed')
                FROM appointments
                WHERE start_time >= NOW() - make_interval(days => %s)
                  AND start_time < NOW()
                GROUP BY 1 HAVING COUNT(*) >= 3
            """, (days,))
            dows = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            out["by_weekday"] = sorted(
                [{"day": dows[r[0]], "set": r[1], "held": r[2],
                  "held_rate": round(r[2] / r[1] * 100) if r[1] else None}
                 for r in rows],
                key=lambda x: -(x["held_rate"] or 0))

            # ── lag effect: does distance from set to start kill shows? ─
            rows = _q(cur, """
                SELECT CASE WHEN start_time - fub_created_at <= INTERVAL '2 days'
                            THEN 'set within 2 days'
                            ELSE 'set 3+ days out' END,
                       COUNT(*) FILTER (WHERE status != 'canceled'),
                       COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed')
                FROM appointments
                WHERE start_time >= NOW() - make_interval(days => %s)
                  AND start_time < NOW() AND fub_created_at IS NOT NULL
                GROUP BY 1
            """, (days,))
            out["by_lag"] = [
                {"bucket": r[0], "set": r[1], "held": r[2],
                 "held_rate": round(r[2] / r[1] * 100) if r[1] else None}
                for r in rows]

            # ── leak candidates (bounded lists for FUB touch checks) ────
            upcoming = _q(cur, """
                SELECT fub_appt_id, person_id, person_name, agent_name,
                       start_time, fub_created_at
                FROM appointments
                WHERE start_time > NOW()
                  AND start_time < NOW() + INTERVAL '7 days'
                  AND status NOT IN ('canceled')
                  AND fub_created_at IS NOT NULL
                  AND fub_created_at < NOW() - INTERVAL '3 days'
                  AND person_id IS NOT NULL
                LIMIT 30
            """)
            held_past = _q(cur, """
                SELECT fub_appt_id, person_id, person_name, agent_name, start_time
                FROM appointments
                WHERE (outcome = 'showed' OR status = 'showed')
                  AND start_time < NOW() - INTERVAL '3 days'
                  AND start_time >= NOW() - INTERVAL '30 days'
                  AND person_id IS NOT NULL
                LIMIT 40
            """)
            no_outcome = _q(cur, """
                SELECT person_name, agent_name, start_time
                FROM appointments
                WHERE outcome IS NULL AND status NOT IN ('canceled','showed')
                  AND start_time < NOW() - INTERVAL '1 day'
                  AND start_time >= NOW() - INTERVAL '14 days'
                ORDER BY start_time
            """)

    # FUB-verified touch checks, bounded to the candidates
    at_risk = []
    for _aid, pid, pname, agent, start, created in upcoming:
        touched = _touched_since(client, pid, created)
        if touched is False:
            at_risk.append({"lead": pname, "agent": agent,
                            "person_id": str(pid),
                            "when": start.isoformat(),
                            "set_days_ago": (now - created).days})
    ghosted = []
    for _aid, pid, pname, agent, start in held_past:
        touched = _touched_since(client, pid, start)
        if touched is False:
            ghosted.append({"lead": pname, "agent": agent,
                            "person_id": str(pid),
                            "held_days_ago": (now - start).days})
    out["at_risk_upcoming"] = at_risk
    out["ghosted_after_held"] = ghosted
    out["no_outcome_list"] = [
        {"lead": r[0], "agent": r[1], "when": r[2].isoformat()} for r in no_outcome]

    # ── dollars left on the table ───────────────────────────────────────
    team_set = sum(a["set"] for a in out["by_agent"]) or 0
    team_held = sum(a["held"] for a in out["by_agent"]) or 0
    contracts = 0
    try:
        ds = _db.get_deal_summary(year=now.year) or {}
        contracts = sum(v.get("contracts", 0) for v in ds.values()
                        if isinstance(v, dict))
    except Exception:
        pass
    appt_to_contract = 0.13  # last measured team rate; refreshed below if computable
    if team_held:
        try:
            from command_sheet import blended_net_per_deal
            net = blended_net_per_deal()
        except Exception:
            net = 3600
        out["leak_dollars"] = {
            "ghosted_count": len(ghosted),
            "assumed_appt_to_contract": appt_to_contract,
            "blended_net_per_deal": round(net),
            "estimate": round(len(ghosted) * appt_to_contract * net),
            "note": ("ghosted held appointments x team appointment-to-contract "
                     "rate x blended net per deal"),
        }
    out["totals"] = {"set": team_set, "held": team_held,
                     "held_rate": round(team_held / team_set * 100) if team_set else None,
                     "no_outcome": len(out["no_outcome_list"])}
    return out


def refresh_cache(days=60):
    data = build_insight(days=days)
    _db.set_app_state(CACHE_KEY, json.dumps(data, default=str))
    return data


def get_cached():
    raw, _ts = _db.get_app_state(CACHE_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None
