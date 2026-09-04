"""
leadstream_insight.py — the intelligence layer behind the LeadStream v2 page.

Barry's brief: "Why would I want to see a list of leads? I want themes,
what's working, which agents are working, where the opportunities are."

Four questions, answered from data we already have:
  1. LEAKS      — where is money sitting on the table right now?
  2. AGENTS     — who works their priority leads and who lets them rot?
  3. SOURCES    — are the leads we buy any good (cost per held appointment,
                  contracts produced)?
  4. POND       — is the nurture machine actually recycling dormant leads?

The per-lead lists survive as drill-down only. No LLM calls; everything is
deterministic and fast. Sections degrade independently on failure.
"""

import logging
from datetime import datetime, timezone

import config
import db as _db

logger = logging.getLogger(__name__)

_NON_AGENTS = set(getattr(config, "EXCLUDED_USERS", []))
_PAUSED = set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))


def _q(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


# ── agent scoreboard + priority-lead leak (from the live tag dashboard) ─────

def build_scoreboard(ls_dashboard):
    """Per-agent follow-up accountability from the LeadStream tag data.
    'actioned' = the agent made an outbound touch since the lead was tagged."""
    rows = []
    for name, a in (ls_dashboard.get("agents") or {}).items():
        if name in _NON_AGENTS or name in _PAUSED:
            continue
        leads = a.get("leads") or []
        tagged = len(leads)
        if not tagged:
            continue
        actioned = sum(1 for l in leads if l.get("actioned"))
        called = sum(1 for l in leads if l.get("called"))
        texted = sum(1 for l in leads if l.get("texted"))
        worked_pct = round(actioned / tagged * 100)
        rows.append({
            "agent": name, "tagged": tagged, "actioned": actioned,
            "called": called, "texted": texted, "worked_pct": worked_pct,
            "grade": "green" if worked_pct >= 70 else
                     "amber" if worked_pct >= 40 else "red",
        })
    rows.sort(key=lambda r: (-r["worked_pct"], -r["tagged"]))
    return rows


# ── leaks ───────────────────────────────────────────────────────────────────

def build_leaks(cur, scoreboard, ls_dashboard):
    leaks = []

    untouched = sum(r["tagged"] - r["actioned"] for r in scoreboard)
    if untouched:
        worst = min(scoreboard, key=lambda r: r["worked_pct"]) if scoreboard else None
        leaks.append({
            "count": untouched,
            "label": "priority leads with zero outbound touch",
            "detail": ("These are the highest-signal leads the engine hand-picked. "
                       + (f"Biggest gap: {worst['agent'].split()[0]} has worked "
                          f"{worst['actioned']} of {worst['tagged']}."
                          if worst else "")),
            "severity": "red" if untouched >= 20 else "amber",
            "action": "Read the scoreboard below, then make it a huddle topic.",
        })

    rows = _q(cur, """
        SELECT COUNT(*) FROM phoenix_log
        WHERE created_at >= CURRENT_DATE - 7 AND status = 'pond_fallback'
    """)
    pool = int(rows[0][0] or 0) if rows else 0
    if pool:
        leaks.append({
            "count": pool,
            "label": "Phoenix leads in the bonus pool, unclaimed (7d)",
            "detail": ("Dead leads that came back to life. Nobody hit the "
                       "personal-dial bar that earns them."),
            "severity": "amber",
            "action": "Remind the team: 4 of 5 weekday dial targets unlocks these.",
        })

    rows = _q(cur, """
        SELECT COUNT(*) FROM isa_transfers
        WHERE transfer_date >= '2026-09-04'
          AND first_call_at IS NULL
          AND transfer_date < NOW() - INTERVAL '24 hours'
    """)
    cold = int(rows[0][0] or 0) if rows else 0
    if cold:
        leaks.append({
            "count": cold,
            "label": "ISA transfers past 24h with no agent call",
            "detail": "Fhalen qualified these live. Every day they sit, they cool.",
            "severity": "red",
            "action": "Name them in the morning huddle. Speed is the whole edge.",
        })

    rows = _q(cur, """
        SELECT COUNT(*) FROM appointments
        WHERE start_time >= CURRENT_DATE - 30
          AND start_time < NOW() - INTERVAL '1 day'
          AND outcome IS NULL AND status NOT IN ('canceled', 'showed')
    """)
    noout = int(rows[0][0] or 0) if rows else 0
    if noout:
        leaks.append({
            "count": noout,
            "label": "appointments (30d) with no logged outcome",
            "detail": "Unlogged outcomes hide both wins and broken follow-up.",
            "severity": "amber",
            "action": "The Tuesday outcome email chases these; escalate repeats.",
        })

    if not leaks:
        leaks.append({"count": 0, "label": "no leaks detected right now",
                      "detail": "Priority leads worked, pool claimed, outcomes logged.",
                      "severity": "green", "action": ""})
    return leaks


# ── lead quality by source ──────────────────────────────────────────────────

def build_sources(cur):
    """Outcome-based source quality: appointments (60d), contracts + GCI (90d),
    against known monthly spend or referral percentage. No volume vanity."""
    econ = getattr(config, "SOURCE_ECONOMICS", {})
    spend = getattr(config, "SOURCE_MONTHLY_SPEND", {})

    appts = {}
    for src, aset, aheld in _q(cur, """
        SELECT COALESCE(NULLIF(source, ''), 'untracked'),
               COUNT(*),
               COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed')
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 60 AND start_time < NOW()
          AND status NOT IN ('canceled')
        GROUP BY 1
    """):
        appts[src] = {"set": int(aset), "held": int(aheld)}

    deals = {}
    for src, contracts, closings, gci in _q(cur, """
        SELECT COALESCE(NULLIF(lead_source, ''), 'untracked'),
               COUNT(*) FILTER (WHERE contract_date >= CURRENT_DATE - 90),
               COUNT(*) FILTER (WHERE close_date >= CURRENT_DATE - 90
                                  AND stage = 'closing'),
               COALESCE(SUM(COALESCE(gci_actual, gci_estimated))
                        FILTER (WHERE close_date >= CURRENT_DATE - 90
                                  AND stage = 'closing'), 0)
        FROM deal_log
        WHERE contract_date >= CURRENT_DATE - 90 OR close_date >= CURRENT_DATE - 90
        GROUP BY 1
    """):
        deals[src] = {"contracts": int(contracts), "closings": int(closings),
                      "gci": float(gci)}

    def _canon(s):
        return (s or "").strip().lower()

    out = []
    for src in sorted(set(appts) | set(deals)):
        a = appts.get(src, {"set": 0, "held": 0})
        d = deals.get(src, {"contracts": 0, "closings": 0, "gci": 0})
        row = {"source": src, **a, **d}
        cs = _canon(src)

        def _match(k):
            ck = _canon(k)
            return cs == ck or cs.startswith(ck) or ck.startswith(cs)

        monthly = next((v for k, v in spend.items() if _match(k)), None)
        ref = next((v.get("referral") for k, v in econ.items() if _match(k)), None)
        # Every Ylopo flavor carries the 40% referral (per Barry, Aug 2026)
        if ref is None and cs.startswith("ylopo"):
            ref = 0.40
        if monthly:
            row["cost_note"] = "$%s/mo" % format(monthly, ",")
            two_months = monthly * 2
            row["cost_per_held"] = round(two_months / a["held"]) if a["held"] else None
        elif ref:
            row["cost_note"] = "%d%% referral at closing" % round(ref * 100)
            row["cost_per_held"] = None
        else:
            row["cost_note"] = "no hard cost"
            row["cost_per_held"] = None
        out.append(row)
    out.sort(key=lambda r: (-r["held"], -r["contracts"]))
    return out


# ── pond health ─────────────────────────────────────────────────────────────

def build_pond(cur, ls_dashboard):
    pond = ls_dashboard.get("pond") or {}
    leads = pond.get("leads") or []
    rows = _q(cur, """
        SELECT (SELECT COUNT(*) FROM pond_email_log
                 WHERE sent_at >= CURRENT_DATE - 30 AND NOT dry_run),
               (SELECT COUNT(*) FROM pond_sms_log
                 WHERE sent_at >= CURRENT_DATE - 30 AND NOT dry_run),
               (SELECT COUNT(*) FROM pond_reply_log
                 WHERE received_at >= CURRENT_DATE - 30),
               (SELECT COUNT(*) FROM phoenix_log
                 WHERE created_at >= CURRENT_DATE - 30
                   AND status IN ('owner_alerted', 'assigned', 'pond_fallback'))
    """)
    emails, sms, replies, revived = ([int(x or 0) for x in rows[0]]
                                     if rows else (0, 0, 0, 0))
    return {
        "size": len(leads),
        "actioned": sum(1 for l in leads if l.get("actioned")),
        "touches_30d": emails + sms,
        "emails_30d": emails, "sms_30d": sms,
        "replies_30d": replies, "revived_30d": revived,
    }


# ── entry point ─────────────────────────────────────────────────────────────

def build_insight(ls_dashboard):
    """ls_dashboard: the JSON from /api/leadstream/dashboard (cached tag
    state). Everything else is direct Postgres."""
    out = {"as_of": datetime.now(timezone.utc).isoformat(),
           "last_run": ls_dashboard.get("last_run"),
           "last_run_mode": ls_dashboard.get("last_run_mode"),
           "sections_failed": []}
    scoreboard = []
    with _db.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                scoreboard = build_scoreboard(ls_dashboard)
            except Exception as e:
                logger.error("insight scoreboard failed: %s", e, exc_info=True)
                out["sections_failed"].append("scoreboard")
            out["scoreboard"] = scoreboard
            for key, fn, args in (
                ("leaks", build_leaks, (cur, scoreboard, ls_dashboard)),
                ("sources", build_sources, (cur,)),
                ("pond", build_pond, (cur, ls_dashboard)),
            ):
                try:
                    out[key] = fn(*args)
                except Exception as e:
                    logger.error("insight %s failed: %s", key, e, exc_info=True)
                    out[key] = [] if key != "pond" else {}
                    out["sections_failed"].append(key)
    return out
