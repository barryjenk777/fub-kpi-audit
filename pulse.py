"""
pulse.py — the 60/90-day themes engine behind the Pulse tab (dashboard v2 home).

Answers Barry's five questions at theme level, from Postgres only (never live
FUB, so it renders in under a second):

    1. Are my agents calling?
    2. Are the conversations happening?
    3. Are conversations turning into appointments?
    4. What happens with the appointments?
    5. Are appointments turning into contracts?

Everything is trailing-window math: last 30 days vs the 30 before it for
deltas, 13 weekly buckets for sparklines. Deterministic theme bullets, no LLM
calls (speed is a feature). Each section is independently fault-tolerant: a
failed query degrades one card, never the page.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone

import config
import db as _db

logger = logging.getLogger(__name__)

# Team totals exclude non-agents; name call-outs also exclude paused agents.
_NON_AGENTS = set(getattr(config, "EXCLUDED_USERS", []))
_NO_CALLOUT = _NON_AGENTS | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))


def _pct_delta(cur, prev):
    """Signed % change, None when prior period has no data to compare."""
    if not prev:
        return None
    return round((cur - prev) / prev * 100)


def _first(name):
    return (name or "").split()[0]


def _q(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


# ── funnel: weekly series + 30v30 ───────────────────────────────────────────

def _funnel(cur):
    """Weekly buckets (13) and 30v30 deltas for the whole pipeline."""
    excl = tuple(_NON_AGENTS) or ("",)

    # calls / convos from daily_activity (nightly FUB sync, per agent per day)
    weekly = {}
    rows = _q(cur, """
        SELECT date_trunc('week', activity_date)::date AS wk,
               SUM(calls_logged), SUM(convos_logged)
        FROM daily_activity
        WHERE activity_date >= CURRENT_DATE - 91
          AND agent_name NOT IN %s
        GROUP BY wk ORDER BY wk
    """, (excl,))
    for wk, calls, convos in rows:
        weekly.setdefault(wk, {})["calls"] = int(calls or 0)
        weekly[wk]["convos"] = int(convos or 0)

    # appointments set / held from the appointments table
    rows = _q(cur, """
        SELECT date_trunc('week', start_time)::date AS wk,
               COUNT(*),
               COUNT(*) FILTER (WHERE outcome = 'showed' OR status = 'showed')
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 91
          AND start_time < NOW()
          AND status NOT IN ('canceled')
        GROUP BY wk ORDER BY wk
    """)
    for wk, aset, aheld in rows:
        weekly.setdefault(wk, {})["appts_set"] = int(aset or 0)
        weekly[wk]["appts_held"] = int(aheld or 0)

    # contracts / closings from deal_log
    rows = _q(cur, """
        SELECT date_trunc('week', contract_date)::date AS wk, COUNT(*)
        FROM deal_log
        WHERE contract_date >= CURRENT_DATE - 91
        GROUP BY wk ORDER BY wk
    """)
    for wk, n in rows:
        weekly.setdefault(wk, {})["contracts"] = int(n or 0)
    rows = _q(cur, """
        SELECT date_trunc('week', close_date)::date AS wk, COUNT(*)
        FROM deal_log
        WHERE close_date >= CURRENT_DATE - 91 AND stage = 'closing'
        GROUP BY wk ORDER BY wk
    """)
    for wk, n in rows:
        weekly.setdefault(wk, {})["closings"] = int(n or 0)

    weeks = sorted(weekly.keys())
    # Drop the current partial week from sparklines so the last point does not
    # look like a cliff (it is just an incomplete week).
    this_week = date.today() - timedelta(days=date.today().weekday())
    spark_weeks = [w for w in weeks if w < this_week] or weeks

    def series(metric):
        return [weekly.get(w, {}).get(metric, 0) for w in spark_weeks]

    # 30 vs prior 30
    def window_sum(sql, params=()):
        cur.execute(sql, params)
        r = cur.fetchone()
        return int(r[0] or 0), int(r[1] or 0)

    calls30, calls_prev = window_sum("""
        SELECT COALESCE(SUM(calls_logged) FILTER (WHERE activity_date >= CURRENT_DATE - 30), 0),
               COALESCE(SUM(calls_logged) FILTER (WHERE activity_date <  CURRENT_DATE - 30), 0)
        FROM daily_activity
        WHERE activity_date >= CURRENT_DATE - 60 AND agent_name NOT IN %s
    """, (excl,))
    convos30, convos_prev = window_sum("""
        SELECT COALESCE(SUM(convos_logged) FILTER (WHERE activity_date >= CURRENT_DATE - 30), 0),
               COALESCE(SUM(convos_logged) FILTER (WHERE activity_date <  CURRENT_DATE - 30), 0)
        FROM daily_activity
        WHERE activity_date >= CURRENT_DATE - 60 AND agent_name NOT IN %s
    """, (excl,))
    set30, set_prev = window_sum("""
        SELECT COUNT(*) FILTER (WHERE start_time >= CURRENT_DATE - 30),
               COUNT(*) FILTER (WHERE start_time <  CURRENT_DATE - 30)
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 60 AND start_time < NOW()
          AND status NOT IN ('canceled')
    """)
    held30, held_prev = window_sum("""
        SELECT COUNT(*) FILTER (WHERE start_time >= CURRENT_DATE - 30),
               COUNT(*) FILTER (WHERE start_time <  CURRENT_DATE - 30)
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 60 AND start_time < NOW()
          AND (outcome = 'showed' OR status = 'showed')
    """)
    con30, con_prev = window_sum("""
        SELECT COUNT(*) FILTER (WHERE contract_date >= CURRENT_DATE - 30),
               COUNT(*) FILTER (WHERE contract_date <  CURRENT_DATE - 30)
        FROM deal_log WHERE contract_date >= CURRENT_DATE - 60
    """)
    clo30, clo_prev = window_sum("""
        SELECT COUNT(*) FILTER (WHERE close_date >= CURRENT_DATE - 30),
               COUNT(*) FILTER (WHERE close_date <  CURRENT_DATE - 30)
        FROM deal_log WHERE close_date >= CURRENT_DATE - 60 AND stage = 'closing'
    """)

    def tile(label, cur30, prev30, metric):
        return {"label": label, "d30": cur30, "prev30": prev30,
                "delta_pct": _pct_delta(cur30, prev30), "series": series(metric)}

    return {
        "weeks": [w.isoformat() for w in spark_weeks],
        "tiles": [
            tile("Calls", calls30, calls_prev, "calls"),
            tile("Conversations", convos30, convos_prev, "convos"),
            tile("Appts set", set30, set_prev, "appts_set"),
            tile("Appts held", held30, held_prev, "appts_held"),
            tile("Contracts", con30, con_prev, "contracts"),
            tile("Closings", clo30, clo_prev, "closings"),
        ],
        "rates": {
            "call_to_convo":  {"d30": round(convos30 / calls30 * 100, 1) if calls30 else None,
                               "prev30": round(convos_prev / calls_prev * 100, 1) if calls_prev else None},
            "convo_to_appt":  {"d30": round(set30 / convos30 * 100, 1) if convos30 else None,
                               "prev30": round(set_prev / convos_prev * 100, 1) if convos_prev else None},
            "held_rate":      {"d30": round(held30 / set30 * 100) if set30 else None,
                               "prev30": round(held_prev / set_prev * 100) if set_prev else None},
            "appt_to_contract": {"d30": round(con30 / held30 * 100) if held30 else None,
                                 "prev30": round(con_prev / held_prev * 100) if held_prev else None},
        },
        "raw": {"calls30": calls30, "convos30": convos30, "set30": set30,
                "held30": held30, "con30": con30, "clo30": clo30},
    }


# ── agent themes ────────────────────────────────────────────────────────────

def _agent_themes(cur, funnel):
    doing, not_doing = [], []
    excl = tuple(_NON_AGENTS) or ("",)
    r = funnel.get("raw", {})
    t = {x["label"]: x for x in funnel.get("tiles", [])}

    # Volume trend
    d = t.get("Calls", {}).get("delta_pct")
    if d is not None:
        (doing if d >= 0 else not_doing).append(
            "Dialing is %s %d%% over the last 30 days (%s calls vs %s)."
            % ("up" if d >= 0 else "down", abs(d),
               "{:,}".format(t["Calls"]["d30"]), "{:,}".format(t["Calls"]["prev30"])))

    # Conversation rate: are the dials turning into talks?
    cr = funnel.get("rates", {}).get("call_to_convo", {})
    if cr.get("d30") is not None and cr.get("prev30") is not None:
        if cr["d30"] >= cr["prev30"]:
            doing.append("Conversation rate improved: %.1f%% of dials become a "
                         "conversation, up from %.1f%%." % (cr["d30"], cr["prev30"]))
        else:
            not_doing.append("Conversations per dial slipped: %.1f%% vs %.1f%% the "
                             "prior month. Volume without conversations is an opener "
                             "problem." % (cr["d30"], cr["prev30"]))

    # The ask: convo -> appt
    ca = funnel.get("rates", {}).get("convo_to_appt", {})
    if ca.get("d30") is not None:
        line = ("%.1f%% of conversations turn into an appointment"
                % ca["d30"])
        if ca.get("prev30") is not None and ca["d30"] < ca["prev30"]:
            not_doing.append(line + " (was %.1f%%). The ask is the lever." % ca["prev30"])
        elif ca["d30"] < 5:
            not_doing.append(line + ". Under 5% means conversations are ending "
                                    "without an ask.")
        else:
            doing.append(line + ".")

    # Consistency: who showed up 4+ days a week, who has gone quiet
    rows = _q(cur, """
        SELECT agent_name,
               COUNT(DISTINCT activity_date) FILTER (
                   WHERE activity_date >= CURRENT_DATE - 28 AND calls_logged > 0) AS active28,
               COALESCE(SUM(calls_logged) FILTER (WHERE activity_date >= CURRENT_DATE - 14), 0) AS calls14
        FROM daily_activity
        WHERE activity_date >= CURRENT_DATE - 28 AND agent_name NOT IN %s
        GROUP BY agent_name
    """, (excl,))
    consistent = [n for n, act, _ in rows if act >= 16]           # 4+ days/wk avg
    silent = sorted(n for n, act, c14 in rows
                    if c14 == 0 and n not in _NO_CALLOUT)
    if consistent:
        doing.append("%d agent%s dialing 4+ days a week for a month straight (%s)."
                     % (len(consistent), "s are" if len(consistent) > 1 else " is",
                        ", ".join(sorted(_first(n) for n in consistent)[:4])))
    if silent:
        not_doing.append("%s: zero calls logged in 14 days. Silence spreads."
                         % " and ".join(_first(n) for n in silent[:3]))

    # Outcome logging discipline
    rows = _q(cur, """
        SELECT COUNT(*) FILTER (WHERE outcome IS NULL AND status NOT IN ('canceled','showed')),
               COUNT(*)
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 30 AND start_time < NOW() - INTERVAL '1 day'
    """)
    if rows and rows[0][1]:
        missing, total = int(rows[0][0]), int(rows[0][1])
        if missing and missing / total > 0.25:
            not_doing.append("%d of %d appointments from the last 30 days have no "
                             "logged outcome. You cannot coach what is not logged."
                             % (missing, total))
        elif total:
            doing.append("Outcome logging is holding: %d of %d recent appointments "
                         "have a logged result." % (total - missing, total))

    return {"doing": doing[:3], "not_doing": not_doing[:3]}


# ── ISA themes ──────────────────────────────────────────────────────────────

def _isa_themes(cur):
    doing, not_doing = [], []
    # Speed-to-transfer is stamped by the FUB call webhook, wired 2026-09-04.
    # Transfers before that date can never have first_call_at, so the 24h
    # metric only counts transfers made since wiring (fair from day one).
    rows = _q(cur, """
        SELECT COUNT(*) FILTER (WHERE transfer_date >= CURRENT_DATE - 30),
               COUNT(*) FILTER (WHERE transfer_date <  CURRENT_DATE - 30
                                  AND transfer_date >= CURRENT_DATE - 60),
               COUNT(*) FILTER (WHERE transfer_date >= '2026-09-04'
                                  AND first_call_at IS NOT NULL),
               COUNT(*) FILTER (WHERE transfer_date >= '2026-09-04'
                                  AND first_call_at IS NOT NULL
                                  AND first_call_at - transfer_date <= INTERVAL '24 hours'),
               COUNT(*) FILTER (WHERE transfer_date >= '2026-09-04')
        FROM isa_transfers
    """)
    t30, tprev, called, called24, tracked = [int(x or 0) for x in rows[0]]

    d = _pct_delta(t30, tprev)
    line = "%d transfers in the last 30 days" % t30
    if d is not None:
        line += " (%s%d%% vs the month before)" % ("+" if d >= 0 else "", d)
    (doing if (d or 0) >= 0 else not_doing).append(line + ".")

    if tracked < 3:
        doing.append("Speed-to-transfer tracking is live as of Sep 4. Every new "
                     "transfer now records how fast the agent calls it; the "
                     "picture builds with each transfer.")
    else:
        pct = round(called / tracked * 100)
        pct24 = round(called24 / tracked * 100)
        if pct24 >= 60:
            doing.append("%d%% of the last %d transfers got an agent call within "
                         "24 hours." % (pct24, tracked))
        else:
            not_doing.append("Only %d%% of the last %d transfers got an agent call "
                             "within 24 hours (%d%% ever did). Fhalen's transfers "
                             "cool fast; same-day follow-up is the whole game."
                             % (pct24, tracked, pct))

    # Transfer type mix
    rows = _q(cur, """
        SELECT COALESCE(transfer_type, 'unknown'), COUNT(*)
        FROM isa_transfers
        WHERE transfer_date >= CURRENT_DATE - 30
        GROUP BY 1 ORDER BY 2 DESC
    """)
    if rows:
        live = sum(n for ty, n in rows if ty == "voice")
        if t30 and live:
            doing.append("%d of %d transfers were live voice handoffs, the "
                         "highest-converting kind." % (live, t30))

    return {"doing": doing[:3], "not_doing": not_doing[:3]}


# ── lead themes ─────────────────────────────────────────────────────────────

def _lead_themes(cur):
    doing, not_doing = [], []

    # Phoenix resurrections
    rows = _q(cur, """
        SELECT COUNT(*) FILTER (WHERE status IN ('owner_alerted','assigned')),
               COUNT(*) FILTER (WHERE status = 'pond_fallback')
        FROM phoenix_log
        WHERE created_at >= CURRENT_DATE - 30 AND status != 'dry_run'
    """)
    routed, pond = [int(x or 0) for x in (rows[0] if rows else (0, 0))]
    if routed or pond:
        doing.append("Phoenix caught %d dead leads that came back to life this "
                     "month; %d routed straight to their agent, %d waiting in the "
                     "bonus pool." % (routed + pond, routed, pond))
    if pond > routed and pond >= 5:
        not_doing.append("%d resurrected leads sat unclaimed because no agent hit "
                         "the personal-dial bar that earns them. Free money on the "
                         "table." % pond)

    # Lead Memory coverage
    rows = _q(cur, """
        SELECT COUNT(*) FILTER (WHERE updated_at >= CURRENT_DATE - 7), COUNT(*)
        FROM lead_briefs
    """)
    if rows:
        fresh7, total = int(rows[0][0] or 0), int(rows[0][1] or 0)
        if total:
            doing.append("Lead Memory keeps a live brief on %d leads (%d refreshed "
                         "this week), so nobody opens a call blind." % (total, fresh7))

    # Pond nurture
    rows = _q(cur, """
        SELECT (SELECT COUNT(*) FROM pond_email_log
                 WHERE sent_at >= CURRENT_DATE - 30 AND NOT dry_run),
               (SELECT COUNT(*) FROM pond_sms_log
                 WHERE sent_at >= CURRENT_DATE - 30 AND NOT dry_run),
               (SELECT COUNT(*) FROM pond_reply_log
                 WHERE received_at >= CURRENT_DATE - 30)
    """)
    emails, sms, replies = [int(x or 0) for x in rows[0]] if rows else (0, 0, 0)
    touches = emails + sms
    if touches:
        line = "Pond nurture sent %d touches this month" % touches
        if replies:
            line += " and pulled %d replies back into play" % replies
        doing.append(line + ".")
    else:
        not_doing.append("Pond nurture sent nothing in 30 days. Those leads are "
                         "aging in place.")

    return {"doing": doing[:3], "not_doing": not_doing[:3]}


# ── appointment themes ──────────────────────────────────────────────────────

def _appt_themes(cur, funnel):
    doing, not_doing = [], []
    r = funnel.get("raw", {})
    rates = funnel.get("rates", {})

    hr = rates.get("held_rate", {})
    if hr.get("d30") is not None:
        if hr["d30"] >= 70:
            doing.append("Held rate is %d%%: at or above the 70%% bar. Confirmations "
                         "are working." % hr["d30"])
        else:
            not_doing.append("Held rate is %d%% against a 70%% bar. Day-before "
                             "confirmation calls are the fix." % hr["d30"])

    ac = rates.get("appt_to_contract", {})
    if ac.get("d30") is not None and r.get("held30"):
        if ac["d30"] >= 20:
            doing.append("%d%% of held appointments turn into a contract within the "
                         "window. The meetings are converting." % ac["d30"])
        else:
            not_doing.append("%d held appointments produced %d contracts (%d%%). "
                             "The meeting happens but the commitment does not; "
                             "that is a closing-skills theme, not an effort theme."
                             % (r["held30"], r.get("con30", 0), ac["d30"]))

    # Upcoming week
    rows = _q(cur, """
        SELECT COUNT(*) FROM appointments
        WHERE start_time >= NOW() AND start_time < NOW() + INTERVAL '7 days'
          AND status NOT IN ('canceled')
    """)
    upcoming = int(rows[0][0] or 0) if rows else 0
    if upcoming:
        doing.append("%d appointments already on the books for the next 7 days." % upcoming)
    else:
        not_doing.append("Nothing on the books for the next 7 days. This week's "
                         "conversations decide next week's calendar.")

    # Source mix
    rows = _q(cur, """
        SELECT COALESCE(NULLIF(source, ''), 'untracked'), COUNT(*)
        FROM appointments
        WHERE start_time >= CURRENT_DATE - 60 AND start_time < NOW()
          AND status NOT IN ('canceled')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 3
    """)
    if rows and rows[0][0] != "untracked":
        doing.append("Top appointment sources (60d): %s."
                     % ", ".join("%s (%d)" % (s, n) for s, n in rows))

    return {"doing": doing[:3], "not_doing": not_doing[:3]}


# ── engine strip: what shipped, what it did in the last 7 days ──────────────

def _engines(cur):
    chips = []

    def chip(name, sql, fmt, params=()):
        try:
            rows = _q(cur, sql, params)
            vals = [int(x or 0) for x in rows[0]] if rows else []
            chips.append({"name": name, "stat": fmt(*vals)})
        except Exception as e:
            logger.warning("pulse engine chip %s failed: %s", name, e)
            chips.append({"name": name, "stat": "status unavailable"})

    chip("Phoenix", """
        SELECT COUNT(*) FILTER (WHERE status IN ('owner_alerted','assigned')),
               COUNT(*) FILTER (WHERE status = 'pond_fallback')
        FROM phoenix_log
        WHERE created_at >= CURRENT_DATE - 7 AND status != 'dry_run'
    """, lambda a, p: "%d revived leads routed, %d in bonus pool (7d)" % (a, p))

    chip("Lead Memory", """
        SELECT COUNT(*) FROM lead_briefs WHERE updated_at >= CURRENT_DATE - 7
    """, lambda n: "%d lead briefs refreshed (7d)" % n)

    chip("Save-Bot", """
        SELECT COUNT(*) FROM savebot_log
        WHERE created_at >= CURRENT_DATE - 7 AND status = 'queued'
    """, lambda n: "%d appointment prep scripts sent (7d)" % n)

    chip("Coaching texts", """
        SELECT COUNT(*) FILTER (WHERE status IN ('sent','sent_email')),
               COUNT(*) FILTER (WHERE status = 'sent_email')
        FROM agent_imessage_queue WHERE created_at >= CURRENT_DATE - 7
    """, lambda s, e: "%d delivered (%d by email) (7d)" % (s, e))

    chip("Pond mailer", """
        SELECT (SELECT COUNT(*) FROM pond_email_log
                 WHERE sent_at >= CURRENT_DATE - 7 AND NOT dry_run),
               (SELECT COUNT(*) FROM pond_sms_log
                 WHERE sent_at >= CURRENT_DATE - 7 AND NOT dry_run)
    """, lambda e, s: "%d emails, %d texts to pond leads (7d)" % (e, s))

    return chips


# ── entry point ─────────────────────────────────────────────────────────────

def build_pulse():
    """Assemble the whole Pulse payload. Postgres only; ~10 fast queries."""
    out = {"as_of": datetime.now(timezone.utc).isoformat(), "sections_failed": []}
    with _db.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                funnel = _funnel(cur)
            except Exception as e:
                logger.error("pulse funnel failed: %s", e, exc_info=True)
                funnel = {"weeks": [], "tiles": [], "rates": {}, "raw": {}}
                out["sections_failed"].append("funnel")
            out["funnel"] = funnel

            for key, fn, args in (
                ("agents", _agent_themes, (cur, funnel)),
                ("isa", _isa_themes, (cur,)),
                ("leads", _lead_themes, (cur,)),
                ("appointments", _appt_themes, (cur, funnel)),
            ):
                try:
                    out.setdefault("themes", {})[key] = fn(*args)
                except Exception as e:
                    logger.error("pulse themes.%s failed: %s", key, e, exc_info=True)
                    out.setdefault("themes", {})[key] = {"doing": [], "not_doing": []}
                    out["sections_failed"].append(key)

            try:
                out["engines"] = _engines(cur)
            except Exception as e:
                logger.error("pulse engines failed: %s", e)
                out["engines"] = []
                out["sections_failed"].append("engines")
    return out
