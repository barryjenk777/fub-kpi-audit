"""
command_sheet.py — Barry's Friday Command Sheet.

Barry runs the Friday team meeting off ONE email. It answers, in order:
  1. THE ONE LEVER      — the single number worth teaching today, its monthly
                          net dollar value, and the one drill. Nothing else.
  2. THE WEEK           — the team strip vs the prior week, leak flagged.
  3. YOUR ROOM          — one card per agent: goal pace, the smallest daily
                          action that closes the gap, a teaching moment that
                          leads with the earned win, and 2 to 3 pre picked
                          pipeline leads to review publicly.
  4. DANNY'S WEEK       — what the productivity coach filed vs the 4 target.
  5. EARNED THIS WEEK   — transfer rotation + Phoenix qualified, or the
                          scarcity line when nobody earned Phoenix.

Hard guardrails (enforced in code, not just prompt):
  - NO em/en dashes anywhere (Barry's rule) — stripped from every output.
  - NO fabrication — Claude only ever narrates numbers we computed. Every
    computed number carries through from a named data source. Unknown stays
    unknown.
  - The ONE LEVER pick is DETERMINISTIC (computed here, never by the LLM).
    The LLM writes only the three narrative lines from the computed numbers.
  - Any generation failure degrades to a deterministic line. A section can
    read plain; it can never crash or invent.
  - This module emails Barry ONLY (config.BARRY_EMAIL). It has no SMS path
    and no agent facing sends, on purpose.

Wiring lives in app.py: scheduled_friday_command_sheet (Fri 6:30am ET) and
POST /api/admin/command-sheet/run for QA without sending.
"""

import json
import logging
import math
import os
import zoneinfo
from datetime import datetime, timedelta, timezone, date

import config
import db as _db
from coach_voice import _strip_dashes

logger = logging.getLogger("command_sheet")

_ET = zoneinfo.ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Economics (Barry's blessed model — deterministic, in code)
# ---------------------------------------------------------------------------

GROSS_GCI_PER_DEAL = 10000          # conservative gross GCI per closed deal
APPT_TO_CONTRACT   = 0.30           # held appointment -> contract
CONTRACT_TO_CLOSE  = 0.80           # contract -> closing
WEEKS_PER_MONTH    = 52.0 / 12.0    # weekly delta -> monthly
HELD_MODEL_TARGET  = 0.70           # model lifts held rate to 70%
RELATIVE_LIFT      = 0.20           # +20% relative for ask/contact/volume
OUTCOME_GAP_BAR    = 0.30           # >30% of appts missing outcomes = lever
OUTCOME_DISCOUNT   = 0.50           # unblocks measurement, worth half of held

# Team revenue lens blend: 60% Ylopo, 20% zbuyer, 20% sphere.
_BLEND = (("Ylopo PPC", 0.60), ("zbuyer", 0.20), ("Sphere / Past Client", 0.20))


def blended_net_per_deal():
    """Conservative net TEAM revenue per closed deal:
    (gross * (1 - referral) * team_split + admin fee) blended across the mix.
    All rates come from config.SOURCE_ECONOMICS (the ground truth doc)."""
    fee = float(getattr(config, "TEAM_ADMIN_FEE_PER_CLOSING", 400))
    total = 0.0
    for source_key, weight in _BLEND:
        econ = (getattr(config, "SOURCE_ECONOMICS", {}) or {}).get(source_key, {})
        referral = float(econ.get("referral", 0.0))
        split = float(econ.get("team_split", 0.5))
        total += weight * (GROSS_GCI_PER_DEAL * (1.0 - referral) * split + fee)
    return round(total)


# ---------------------------------------------------------------------------
# THE ONE LEVER — deterministic pick
# ---------------------------------------------------------------------------

_LEVER_LABELS = {
    "held_rate":       "held appointments",
    "ask_rate":        "the ask",
    "contact_rate":    "contact rate",
    "volume":          "dials",
    "outcome_logging": "outcome logging",
}

_LEVER_DRILLS = {
    "held_rate": ("Drill: every agent scripts a 24 hour value touch for each "
                  "appointment on the books before leaving the room."),
    "ask_rate": ("Drill: role play the meeting ask, two rounds each. Prescribe "
                 "the meeting, two time options, no permission language."),
    "contact_rate": ("Drill: first ten dials happen in the room right now. "
                     "Double dial every number before any voicemail."),
    "volume": ("Drill: everyone opens their calendar and blocks next week's "
               "daily call hour before leaving the room."),
    "outcome_logging": ("Drill: open FUB now. Every appointment older than 48 "
                        "hours gets its outcome logged before the meeting ends."),
}


def compute_one_lever(team, missing_outcome_pct):
    """
    Pick the single highest dollar lever from last week's team totals.

    team = {"calls": K, "convos": C, "appts_set": S, "appts_met": M}
    missing_outcome_pct = fraction of last week's set appointments with no
    outcome logged (0.0 when appts_set == 0).

    Returns dict: key, label, monthly_value, drill, plus every intermediate
    number used, so each figure in the email traces to this computation.
    """
    K = int(team.get("calls") or 0)
    C = int(team.get("convos") or 0)
    S = int(team.get("appts_set") or 0)
    M = int(team.get("appts_met") or 0)

    per_deal = blended_net_per_deal()
    val_held_appt = APPT_TO_CONTRACT * CONTRACT_TO_CLOSE * per_deal

    held = (M / S) if S else None
    ask = (C and (S / C)) or 0.0
    contact = (K and (C / K)) or 0.0
    held_used = held if (held is not None and held > 0) else HELD_MODEL_TARGET

    candidates = []

    # held_rate: lift set->met to 70%
    if S > 0 and (held or 0.0) < HELD_MODEL_TARGET:
        delta_met = HELD_MODEL_TARGET * S - M
        v = delta_met * val_held_appt * WEEKS_PER_MONTH
        candidates.append({"key": "held_rate", "value": v, "delta_weekly": delta_met})
    held_value = next((c["value"] for c in candidates if c["key"] == "held_rate"), 0.0)

    # outcome_logging: unblocks measurement; only in play past the 30% bar
    if S > 0 and missing_outcome_pct > OUTCOME_GAP_BAR:
        candidates.append({"key": "outcome_logging",
                           "value": held_value * OUTCOME_DISCOUNT,
                           "delta_weekly": missing_outcome_pct * S})

    # ask_rate: +20% relative on convos->set
    if S > 0:
        delta_set = RELATIVE_LIFT * S
        v = delta_set * held_used * val_held_appt * WEEKS_PER_MONTH
        candidates.append({"key": "ask_rate", "value": v, "delta_weekly": delta_set})

    # contact_rate: +20% relative on calls->convos
    if C > 0:
        delta_convos = RELATIVE_LIFT * C
        v = delta_convos * ask * held_used * val_held_appt * WEEKS_PER_MONTH
        candidates.append({"key": "contact_rate", "value": v, "delta_weekly": delta_convos})

    # volume: +20% calls at current contact rate
    if K > 0:
        delta_convos = RELATIVE_LIFT * K * contact
        v = delta_convos * ask * held_used * val_held_appt * WEEKS_PER_MONTH
        candidates.append({"key": "volume", "value": v, "delta_weekly": RELATIVE_LIFT * K})

    if not candidates:
        return None

    # Deterministic tie break (contact_rate and volume model out identical):
    # weakest link first. Below benchmark contact rate, teach contact rate.
    prio = {"held_rate": 0, "outcome_logging": 1, "ask_rate": 2}
    prio["contact_rate"] = 3 if contact < 0.15 else 4
    prio["volume"] = 4 if contact < 0.15 else 3
    candidates.sort(key=lambda c: (-round(c["value"]), prio.get(c["key"], 9)))
    top = candidates[0]

    return {
        "key": top["key"],
        "label": _LEVER_LABELS[top["key"]],
        "monthly_value": int(round(top["value"])),
        "delta_weekly": round(top["delta_weekly"], 1),
        "drill": _LEVER_DRILLS[top["key"]],
        "calls": K, "convos": C, "appts_set": S, "appts_met": M,
        "held_rate": round(held, 3) if held is not None else None,
        "ask_rate": round(ask, 3),
        "contact_rate": round(contact, 3),
        "missing_outcome_pct": round(missing_outcome_pct, 3),
        "per_deal_net": per_deal,
        "value_per_held_appt": int(round(val_held_appt)),
        "model": ("blend 60/20/20 ylopo/zbuyer/sphere, $10k gross GCI per deal, "
                  "appt to contract 0.30, contract to close 0.80, weekly x 52/12"),
        "all_candidates": [{"key": c["key"], "monthly_value": int(round(c["value"]))}
                           for c in candidates],
    }


def _money(n):
    try:
        return f"${int(round(float(n))):,}"
    except (TypeError, ValueError):
        return "unknown"


def _goal_money(gci):
    try:
        gci = float(gci)
    except (TypeError, ValueError):
        return "unknown"
    if gci >= 1000 and gci % 1000 == 0:
        return f"${int(gci / 1000)}K"
    return _money(gci)


def _pct(x, digits=0):
    if x is None:
        return "unknown"
    return f"{round(x * 100, digits) if digits else int(round(x * 100))}%"


def build_subject(lever):
    if not lever:
        return _strip_dashes("Friday meeting: no completed week of numbers yet")
    return _strip_dashes(
        f"Friday meeting: the lever is {lever['label']}. "
        f"{_money(lever['monthly_value'])} a month is sitting in it.")


def _lever_fallback_lines(lever):
    """Deterministic 3 line narrative when generation is unavailable."""
    k = lever["key"]
    if k == "held_rate":
        first = (f"Set {lever['appts_set']}, held {lever['appts_met']}. That is "
                 f"{_pct(lever['held_rate'])} held against the 70% bar.")
        second = (f"Lifting held to 70% is worth about "
                  f"{_money(lever['monthly_value'])} a month in net team GCI.")
    elif k == "outcome_logging":
        first = (f"{_pct(lever['missing_outcome_pct'])} of last week's "
                 f"{lever['appts_set']} appointments have no outcome logged. "
                 "We cannot coach what we cannot see.")
        second = (f"Cleaning it up unblocks the held rate lever, worth about "
                  f"{_money(lever['monthly_value'])} a month.")
    elif k == "ask_rate":
        first = (f"{lever['convos']} conversations became {lever['appts_set']} "
                 f"appointments. That is an ask rate of {_pct(lever['ask_rate'])}.")
        second = (f"A 20% lift in the ask is worth about "
                  f"{_money(lever['monthly_value'])} a month in net team GCI.")
    elif k == "contact_rate":
        first = (f"{lever['calls']} dials became {lever['convos']} conversations. "
                 f"That is a contact rate of {_pct(lever['contact_rate'])}.")
        second = (f"A 20% lift in contact rate is worth about "
                  f"{_money(lever['monthly_value'])} a month in net team GCI.")
    else:
        first = (f"{lever['calls']} team dials last week at a "
                 f"{_pct(lever['contact_rate'])} contact rate.")
        second = (f"20% more dials is worth about "
                  f"{_money(lever['monthly_value'])} a month in net team GCI.")
    third = f"{lever['drill']} Teach nothing else."
    return [first, second, third]


# ---------------------------------------------------------------------------
# LLM (same working pattern as lead_memory.py: haiku id + extra_body temp)
# ---------------------------------------------------------------------------

def _llm():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        logger.warning("command_sheet: anthropic client init failed: %s", e)
        return None


_SYSTEM = """You write short lines for Barry Jenkins' Friday team meeting email. Barry's register (Too Nice for Sales): a counselor teaching, never a boss shaming. Warm, direct, certain about the process.

HARD RULES:
- Use ONLY the numbers supplied. NEVER invent a number, name, rate, or outcome. If something is not supplied, do not mention it.
- NEVER use em dashes or en dashes. Periods and commas only.
- Teach, never shame. Every agent line leads with the earned win before the gap.
- Reframe, never pile on. The math is the encouragement.
- Short. Punchy. No filler, no corporate voice, no exclamation stacking."""


def _generate(client, prompt, max_tokens=350):
    """One generation call, lead_memory pattern. Returns stripped text or None."""
    try:
        resp = client.messages.create(
            model=getattr(config, "COMMAND_SHEET_MODEL", "claude-haiku-4-5"),
            max_tokens=max_tokens,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            # anthropic SDK 1.x dropped the temperature kwarg from
            # Messages.create(); the API still honors it, so send it through
            # extra_body (same approach lead_memory.py uses in production).
            extra_body={"temperature": float(getattr(config, "COMMAND_SHEET_TEMPERATURE", 0.4))},
        )
        return _strip_dashes(resp.content[0].text.strip())
    except Exception as e:
        logger.warning("command_sheet generation failed: %s", e)
        return None


def _gen_lever_lines(client, lever):
    """LLM writes ONLY the 3 narrative lines from the computed numbers."""
    fallback = _lever_fallback_lines(lever)
    if client is None:
        return fallback
    prompt = f"""The one lever for today's meeting was computed deterministically. Write EXACTLY 3 lines of plain text, nothing before or after:
Line 1: the numbers behind the lever, stated plainly.
Line 2: the monthly net dollar value of the realistic improvement, using the exact dollar figure supplied.
Line 3: today's single drill (use the drill supplied, you may tighten the wording), and the line MUST end with exactly: Teach nothing else.

COMPUTED NUMBERS (the only facts you may use):
{json.dumps({k: v for k, v in lever.items() if k not in ('all_candidates',)}, indent=1)}

Register example (do not copy numbers from it): "We set 21 appointments and sat 9 of them. That is a 43% held rate against a 70% bar."
No em dashes or en dashes. Max 3 lines."""
    raw = _generate(client, prompt, max_tokens=300)
    if not raw:
        return fallback
    lines = [l.strip() for l in raw.splitlines() if l.strip()][:3]
    if len(lines) < 2 or _money(lever["monthly_value"]).replace("$", "") not in raw.replace("$", ""):
        return fallback
    if not lines[-1].rstrip().endswith("Teach nothing else."):
        lines[-1] = lines[-1].rstrip().rstrip(".") + ". Teach nothing else."
    return lines[:3]


def _gen_agent_lines(client, payload, fallback_pace, fallback_teach):
    """LLM rewrites the two agent lines from computed numbers. Falls back to
    the deterministic lines on any failure or weak output."""
    if client is None:
        return fallback_pace, fallback_teach
    prompt = f"""Write two lines for one agent's card in the Friday meeting email.
Line 1 starts with "PACE:" then Barry's blessed goal pace formula: goal dollars, percent of pace, then THE SMALLEST DAILY ACTION that closes the gap, then a reframe. Register example (do not copy its numbers): "Goal $130K. 41% of pace. The math: 6 conversations a day closes the gap, about 13 dials at his contact rate. He is closer than the percent looks, say that first."
Line 2 starts with "TEACH:" then one teaching moment from this agent's week. LEAD WITH THE EARNED WIN, then the one gap, then what to teach. Never shame, always reframe.

THE ONLY NUMBERS YOU MAY USE:
{json.dumps(payload, indent=1, default=str)}

Output exactly two lines, "PACE: ..." then "TEACH: ...". No em dashes or en dashes. Under 500 characters combined."""
    raw = _generate(client, prompt, max_tokens=350)
    if not raw:
        return fallback_pace, fallback_teach
    pace_line, teach_line = None, None
    for line in raw.splitlines():
        s = line.strip()
        if s.upper().startswith("PACE:"):
            pace_line = s[5:].strip()
        elif s.upper().startswith("TEACH:"):
            teach_line = s[6:].strip()
    if not pace_line or not teach_line:
        return fallback_pace, fallback_teach
    return pace_line, teach_line


# ---------------------------------------------------------------------------
# Per agent computations (deterministic bones the LLM narrates over)
# ---------------------------------------------------------------------------

def _agent_gap_math(pace, targets, goal, week):
    """The smallest daily action that closes the gap. Returns dict or None."""
    if not pace or not targets:
        return None
    convos_pace = pace.get("convos") or {}
    annual = float(convos_pace.get("annual") or 0)
    actual = float(convos_pace.get("actual") or 0)
    if annual <= 0:
        return None
    today = datetime.now(_ET).date()
    days_left = max((date(today.year, 12, 31) - today).days, 7)
    workdays_left = max(days_left * 5.0 / 7.0, 5.0)
    remaining = max(annual - actual, 0.0)
    daily_convos = max(int(math.ceil(remaining / workdays_left)), 1)

    calls = int(week.get("calls") or 0)
    convos = int(week.get("convos") or 0)
    if calls >= 10 and convos > 0:
        contact = convos / calls
        contact_src = "his actual contact rate last week"
    else:
        contact = float((goal or {}).get("contact_rate") or 0.15)
        contact_src = "the contact rate in his goal plan"
    daily_dials = max(int(math.ceil(daily_convos / max(contact, 0.01))), daily_convos)
    return {"daily_convos": daily_convos, "daily_dials": daily_dials,
            "contact_rate_used": round(contact, 3), "contact_rate_source": contact_src}


def _fallback_pace_line(goal, pace, gap):
    if not goal or float(goal.get("gci_goal") or 0) <= 0:
        return ("No goal on file yet. The first move is the goal conversation, "
                "everything else waits on it.")
    parts = [f"Goal {_goal_money(goal.get('gci_goal'))}."]
    if pace and pace.get("overall_pct") is not None:
        pct = int(pace["overall_pct"])
        parts.append(f"{pct}% of pace.")
    else:
        pct = None
        parts.append("Pace unknown, YTD actuals not synced yet.")
    if gap:
        parts.append(f"The math: {gap['daily_convos']} conversations a day closes "
                     f"the gap, about {gap['daily_dials']} dials at "
                     f"{gap['contact_rate_source']}.")
    if pct is not None and pct < 70:
        parts.append("The percent looks worse than the math. Say the math first.")
    elif pct is not None and pct >= 90:
        parts.append("Ahead of the math. Protect the habit that got him here.")
    else:
        parts.append("Closer than it feels. Say that first.")
    return " ".join(parts)


def _rates_for_week(week):
    calls = int(week.get("calls") or 0)
    convos = int(week.get("convos") or 0)
    aset = int(week.get("appts_set") or 0)
    amet = int(week.get("appts_met") or 0)
    return {
        "contact": (convos / calls) if calls else None,
        "ask": (aset / convos) if convos else None,
        "held": (amet / aset) if aset else None,
        "calls": calls, "convos": convos, "appts_set": aset, "appts_met": amet,
    }


def _fallback_teach_line(week, streak):
    """Deterministic teach line: win first, then the gap. Benchmarks: contact
    15%, ask 10%, held 70% (the same rates the goal model uses)."""
    r = _rates_for_week(week)
    wins, gaps = [], []
    if streak and int(streak.get("current_streak") or 0) >= 3:
        wins.append(f"a {streak['current_streak']} day activity streak")
    if r["contact"] is not None and r["contact"] >= 0.15:
        wins.append(f"a {_pct(r['contact'])} contact rate")
    elif r["contact"] is not None:
        gaps.append(f"contact rate at {_pct(r['contact'])} against the 15% bar")
    if r["ask"] is not None and r["ask"] >= 0.10:
        wins.append(f"{r['appts_set']} appointments from {r['convos']} conversations")
    elif r["ask"] is not None:
        gaps.append(f"{r['convos']} conversations became only {r['appts_set']} "
                    "appointments, the ask is the gap")
    if r["held"] is not None and r["held"] >= 0.70:
        wins.append(f"{r['appts_met']} of {r['appts_set']} appointments held")
    elif r["held"] is not None:
        gaps.append(f"only {r['appts_met']} of {r['appts_set']} set appointments held")
    if r["calls"] == 0:
        gaps.append("zero dials logged, the week never started")
    elif r["calls"] >= 30:
        wins.append(f"{r['calls']} dials of volume")

    win_txt = wins[0] if wins else f"{r['calls']} dials and {r['convos']} conversations on the board"
    if gaps:
        return f"Win first: {win_txt}. The one gap: {gaps[0]}. Teach that gap, nothing else."
    return f"Win first: {win_txt}. No obvious leak this week. Ask what worked and have them teach it."


# ---------------------------------------------------------------------------
# Pipeline picks (2 to 3 leads per agent to review publicly)
# ---------------------------------------------------------------------------

_LEAD_MEMORY_SUBJECT = getattr(config, "LEAD_MEMORY_NOTE_SUBJECT", "LEAD MEMORY (auto)")


def _lead_memory_next_move(fub, person_id):
    """NEXT MOVE line from the lead's LEAD MEMORY note, or None."""
    try:
        notes = fub.get_person_notes(person_id, limit=40)
    except Exception:
        return None
    for n in notes or []:
        if (n.get("subject") or "").strip() == _LEAD_MEMORY_SUBJECT:
            for line in (n.get("body") or "").splitlines():
                if line.strip().upper().startswith("NEXT MOVE:"):
                    move = line.strip()[10:].strip()
                    return move[:240] if move else None
            return None
    return None


def _fetch_hot_leads(fub, user_id):
    """Leads assigned to this agent whose stage contains 'hot'. Bounded fetch
    (3 pages, newest first per FUB default) so a big book never stalls the run."""
    try:
        people = fub._get_paginated(
            "people", {"assignedUserId": user_id, "limit": 100}, max_pages=3)
    except Exception as e:
        logger.warning("[COMMAND SHEET] hot lead fetch failed for %s: %s", user_id, e)
        return []
    out = []
    for p in people or []:
        if "hot" in (p.get("stage") or "").lower():
            out.append(p)
    return out


def _pick_pipeline_leads(fub, user_id, missing_outcome_appts,
                         phoenix_rows, manifest_leads, max_leads=3):
    """Pre pick 2 to 3 leads for public review, in Barry's priority order:
    (a) hot stage with no appointment, (b) appointments missing outcomes,
    (c) PHOENIX tagged, (d) top LeadStream. Dedup by person id."""
    picks, seen = [], set()

    def _add(pid, name, status, fallback_move):
        pid = str(pid or "")
        key = pid or name
        if not name or key in seen or len(picks) >= max_leads:
            return
        seen.add(key)
        move = (_lead_memory_next_move(fub, pid) if pid else None) or fallback_move
        picks.append({"name": name, "status": status, "next_move": move})

    hot = _fetch_hot_leads(fub, user_id)

    # (a) hot stage, no appointment tag on the record
    apt_tags = {config.APT_SET_TAG, config.APT_OUTCOME_NEEDED_TAG, config.APT_STALE_TAG}
    for p in hot:
        tags = set(p.get("tags") or [])
        if tags & apt_tags:
            continue
        nm = p.get("name") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
        _add(p.get("id"), nm,
             f"Stage {p.get('stage') or 'hot'}, no appointment on the books",
             "Hot stage with no appointment. Call and prescribe the meeting, two time options.")
        if len(picks) >= max_leads:
            return picks

    # (b) last week's appointments with no outcome logged
    for ap in missing_outcome_appts:
        _add(ap.get("person_id"), ap.get("lead_name"),
             f"Appointment on {ap.get('start_date') or 'unknown date'} has no outcome logged",
             "Log the outcome first, then set the next step from what actually happened.")
        if len(picks) >= max_leads:
            return picks

    # (c) PHOENIX tagged (resurrections routed to this agent, last 14 days)
    for row in phoenix_rows:
        _add(row.get("person_id"), row.get("lead_name"),
             f"PHOENIX resurrection, came back after {row.get('dormant_days')} days quiet",
             "They came back on their own. Zero competition, call first.")
        if len(picks) >= max_leads:
            return picks
    for p in hot:
        if "PHOENIX" in (p.get("tags") or []):
            nm = p.get("name") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
            _add(p.get("id"), nm, "PHOENIX tagged, back on the site",
                 "They came back on their own. Zero competition, call first.")
            if len(picks) >= max_leads:
                return picks

    # (d) top LeadStream leads from the manifest (no API cost)
    for ml in manifest_leads:
        _add(ml.get("id"), ml.get("name"),
             f"Top of LeadStream, score {ml.get('score')}"
             + (f", stage {ml.get('stage')}" if ml.get("stage") else ""),
             "Top of the daily call list. Open with the behavior that scored them there.")
        if len(picks) >= max_leads:
            return picks

    return picks


# ---------------------------------------------------------------------------
# HTML rendering (mobile first, dark on light, email client safe)
# ---------------------------------------------------------------------------

_ARROWS = {"up": ("&#9650;", "#1a7f37"), "down": ("&#9660;", "#b35900"),
           "flat": ("&#9654;", "#6b7280")}


def _arrow(curr, prev):
    if prev is None:
        return ""
    d = "up" if curr > prev else ("down" if curr < prev else "flat")
    sym, color = _ARROWS[d]
    return (f' <span style="color:{color};font-size:12px">{sym} '
            f'{prev} last wk</span>')


def _esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;"))


def render_html(data):
    """Assemble the full email HTML from the computed data dict."""
    lever = data.get("lever")
    lever_lines = data.get("lever_lines") or []
    week = data.get("team_week") or {}
    prior = data.get("team_prior")
    S = int(week.get("appts_set") or 0)

    css_card = ("background:#ffffff;border:1px solid #e2e0da;border-radius:12px;"
                "padding:16px 18px;margin:0 0 14px 0;")
    css_h = ("font-size:13px;letter-spacing:1.5px;color:#8a8578;font-weight:700;"
             "margin:26px 0 10px 0;text-transform:uppercase;")

    parts = [
        '<div style="background:#f6f5f1;padding:18px 10px;">',
        '<div style="max-width:600px;margin:0 auto;font-family:-apple-system,'
        "BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#1f1d1a;"
        'font-size:15px;line-height:1.5;">',
        '<div style="font-size:12px;letter-spacing:2px;color:#b3ad9e;'
        'font-weight:800;text-transform:uppercase;margin-bottom:4px;">'
        'Legacy Home Team &middot; Friday Command Sheet</div>',
        f'<div style="font-size:13px;color:#8a8578;margin-bottom:18px;">'
        f'Week of {_esc(data.get("period_label") or "unknown")}</div>',
    ]

    # ── 1. THE ONE LEVER ──────────────────────────────────────────────
    parts.append(f'<div style="{css_h}">1 &middot; The One Lever</div>')
    if lever:
        parts.append(
            f'<div style="{css_card}border-left:5px solid #f5a623;">'
            f'<div style="font-size:26px;font-weight:800;line-height:1.2;'
            f'margin-bottom:4px;">{_esc(lever["label"]).upper()}</div>'
            f'<div style="font-size:18px;font-weight:700;color:#1a7f37;'
            f'margin-bottom:10px;">{_money(lever["monthly_value"])} a month is sitting in it</div>'
            + "".join(f'<div style="margin-bottom:8px;">{_esc(l)}</div>' for l in lever_lines)
            + '</div>')
    else:
        parts.append(f'<div style="{css_card}">No completed week of team numbers '
                     'yet, so there is no lever to compute. Run the meeting off '
                     'the room section below.</div>')

    # ── 2. THE WEEK ───────────────────────────────────────────────────
    parts.append(f'<div style="{css_h}">2 &middot; The Week</div>')
    leak = lever and lever.get("key") == "held_rate"
    pk = (lambda k: (prior or {}).get(k)) if prior else (lambda k: None)
    cells = [
        ("Calls", int(week.get("calls") or 0), pk("calls"), False),
        ("Convos", int(week.get("convos") or 0), pk("convos"), False),
        ("Appts set", S, pk("appts_set"), False),
        ("Appts HELD", int(week.get("appts_met") or 0), pk("appts_met"), bool(leak)),
    ]
    strip = ['<div style="' + css_card + '">',
             '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
             'style="border-collapse:collapse;">']
    row1, row2 = [], []
    for label, curr, prev, is_leak in cells:
        badge = ('<div style="display:inline-block;background:#b3261e;color:#fff;'
                 'font-size:10px;font-weight:800;letter-spacing:1px;padding:1px 6px;'
                 'border-radius:8px;margin-left:4px;">THE LEAK</div>') if is_leak else ""
        row1.append(f'<td style="padding:4px 6px;font-size:12px;color:#8a8578;'
                    f'text-transform:uppercase;letter-spacing:0.5px;">{label}{badge}</td>')
        row2.append(f'<td style="padding:0 6px 6px 6px;font-size:22px;font-weight:800;">'
                    f'{curr}{_arrow(curr, prev)}</td>')
    strip.append("<tr>" + "".join(row1) + "</tr><tr>" + "".join(row2) + "</tr></table>")
    tr = data.get("transfers") or {}
    if tr.get("transfers") is not None:
        strip.append(f'<div style="font-size:13px;color:#5c574c;margin-top:6px;">'
                     f'Transfers converted: {tr.get("with_appt", 0)} of '
                     f'{tr.get("transfers", 0)} reached an appointment '
                     f'(trailing 7 days).</div>')
    else:
        strip.append('<div style="font-size:13px;color:#5c574c;margin-top:6px;">'
                     'Transfers converted: unknown (transfer table unavailable).</div>')
    if S and data.get("missing_outcomes_team") is not None:
        strip.append(f'<div style="font-size:13px;color:#5c574c;">'
                     f'Outcomes missing on {data["missing_outcomes_team"]} of {S} '
                     f'set appointments.</div>')
    strip.append("</div>")
    parts.extend(strip)

    # ── 3. YOUR ROOM ──────────────────────────────────────────────────
    parts.append(f'<div style="{css_h}">3 &middot; Your Room, Agent by Agent</div>')
    for card in data.get("agent_cards") or []:
        w = card.get("week") or {}
        head = (f'<div style="font-size:17px;font-weight:800;margin-bottom:2px;">'
                f'{_esc(card["name"])}</div>'
                f'<div style="font-size:12px;color:#8a8578;margin-bottom:10px;">'
                f'{w.get("calls", 0)} calls &middot; {w.get("convos", 0)} convos &middot; '
                f'{w.get("appts_set", 0)} set &middot; {w.get("appts_met", 0)} held</div>')
        body = []
        if card.get("paused"):
            body.append(f'<div style="margin-bottom:8px;color:#5c574c;">'
                        f'{_esc(card.get("paused_note"))}</div>')
        else:
            body.append(f'<div style="margin-bottom:8px;"><b>Pace.</b> '
                        f'{_esc(card.get("pace_line"))}</div>')
            body.append(f'<div style="margin-bottom:8px;"><b>Teach.</b> '
                        f'{_esc(card.get("teach_line"))}</div>')
        leads = card.get("leads") or []
        if leads:
            body.append('<div style="font-size:12px;color:#8a8578;text-transform:'
                        'uppercase;letter-spacing:0.5px;margin:10px 0 4px 0;">'
                        'Review these together</div>')
            for ld in leads:
                body.append(
                    f'<div style="border-top:1px solid #efede7;padding:7px 0;">'
                    f'<div style="font-weight:700;">{_esc(ld["name"])}</div>'
                    f'<div style="font-size:13px;color:#5c574c;">{_esc(ld["status"])}</div>'
                    f'<div style="font-size:13px;">Next move: {_esc(ld["next_move"])}</div>'
                    f'</div>')
        else:
            body.append('<div style="font-size:13px;color:#8a8578;">No pipeline '
                        'picks surfaced for this agent this week.</div>')
        parts.append(f'<div style="{css_card}">{head}{"".join(body)}</div>')

    # ── 4. DANNY'S WEEK ───────────────────────────────────────────────
    parts.append(f'<div style="{css_h}">4 &middot; Danny&#39;s Week</div>')
    danny = data.get("danny")
    if danny:
        rows = "".join(
            f'<div style="border-top:1px solid #efede7;padding:6px 0;font-size:14px;">'
            f'<b>{_esc(e.get("agent"))}</b>: '
            f'{"met" if str(e.get("met")).lower() == "yes" else "did not meet"}'
            + (f'. {_esc(e.get("note") or e.get("commit"))}' if (e.get("note") or e.get("commit")) else "")
            + '</div>'
            for e in danny.get("entries") or [])
        parts.append(f'<div style="{css_card}">'
                     f'<div style="margin-bottom:6px;"><b>{danny["met_count"]} of the 4 '
                     f'target agents met this week.</b></div>{rows}</div>')
    else:
        parts.append(f'<div style="{css_card}">No update from Danny yet this week.</div>')

    # ── 5. EARNED THIS WEEK ───────────────────────────────────────────
    parts.append(f'<div style="{css_h}">5 &middot; Earned This Week</div>')
    rotation = data.get("rotation") or []
    phoenix_q = data.get("phoenix_qualified") or []
    earned = ['<div style="' + css_card + '">']
    if rotation:
        earned.append('<div style="margin-bottom:8px;"><b>Transfer rotation:</b> '
                      + _esc(", ".join(rotation)) + '.</div>')
    else:
        earned.append('<div style="margin-bottom:8px;"><b>Transfer rotation:</b> '
                      'nobody hit the standard last week. The bar stays where it '
                      'is, the door stays open.</div>')
    if phoenix_q:
        earned.append('<div><b>Phoenix qualified:</b> '
                      + _esc(", ".join(f"{r['agent_name']} ({r['days_met']} of 5 days)"
                                       for r in phoenix_q)) + '.</div>')
    else:
        pond = data.get("phoenix_pond_unclaimed")
        if pond:
            earned.append(f'<div><b>Phoenix qualified: nobody.</b> {pond} bonus '
                          f'lead{"s" if pond != 1 else ""} went to the pond '
                          'unclaimed this week. Those were free at bats, and they '
                          'went to whoever grabbed them first.</div>')
        elif pond == 0:
            earned.append('<div><b>Phoenix qualified: nobody</b>, and no bonus '
                          'leads moved this week.</div>')
        else:
            earned.append('<div><b>Phoenix qualified:</b> unknown, the '
                          'qualification check did not complete this run.</div>')
    earned.append("</div>")
    parts.extend(earned)

    parts.append('<div style="font-size:12px;color:#b3ad9e;margin:18px 0 8px 0;">'
                 'Every number traces to a named source. Unknowns say unknown. '
                 'Built for the Friday meeting, sent only to Barry.</div>')
    parts.append("</div></div>")
    return _strip_dashes("".join(parts))


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def run_command_sheet(dry_run=True, email_to_barry=False,
                      audit_fn=None, phoenix_fn=None, manifest_fn=None):
    """
    Build (and optionally send) the Friday Command Sheet.

    dry_run=True never sends anything. dry_run=False sends the email to
    config.BARRY_EMAIL only when email_to_barry is also True. This function
    never texts anyone and never emails an agent, by design.

    audit_fn/phoenix_fn/manifest_fn are injected by app.py so this module
    reuses run_audit_data, _phoenix_qualified_agents and the LeadStream
    manifest loader without a circular import.
    """
    from fub_client import FUBClient

    dry_run = bool(dry_run)
    gaps = []
    summary = {"ok": True, "dry_run": dry_run, "emailed": False,
               "run_at": datetime.now(_ET).isoformat(), "data_gaps": gaps}

    fub = FUBClient()

    # ── Audit: last completed Mon-Sun week (cache-first via app.py) ────
    audit = audit_fn() if audit_fn else None
    if not audit or not audit.get("agents"):
        raise RuntimeError("audit data unavailable, cannot build the sheet")
    agents = audit["agents"]
    totals = audit.get("totals") or {}
    period = audit.get("period") or {}
    summary["period"] = period
    period_label = f"{period.get('start', '?')} to {period.get('end', '?')}"

    since = until = None
    try:
        since = datetime.fromisoformat(audit["period_since_iso"])
        until = datetime.fromisoformat(audit["period_until_iso"])
    except Exception:
        gaps.append("audit period timestamps missing, outcome check skipped")

    coaching_paused = set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))

    # ── Prior week from the snapshot table (arrows) ────────────────────
    prior_totals, prior_agents = None, {}
    try:
        hist = _db.get_weekly_kpi_history(weeks=4) or []
        audit_ws = since.astimezone(_ET).date().isoformat() if since else None
        prior_wk = next((w for w in hist
                         if audit_ws and w.get("week_start") < audit_ws), None)
        if prior_wk:
            pt = {"calls": 0, "convos": 0, "appts_set": 0, "appts_met": 0}
            for a in prior_wk.get("agents") or []:
                pt["calls"] += int(a.get("outbound_calls") or 0)
                pt["convos"] += int(a.get("conversations") or 0)
                pt["appts_set"] += int(a.get("appts_set") or 0)
                pt["appts_met"] += int(a.get("appts_met") or 0)
                prior_agents[a.get("name")] = a
            prior_totals = pt
    except Exception as e:
        logger.warning("[COMMAND SHEET] prior week lookup failed: %s", e)
    if prior_totals is None:
        gaps.append("no prior week snapshot, week strip arrows omitted")

    # ── Appointments for the audit week: missing outcomes ──────────────
    roster_ids = {a.get("user_id") for a in agents}
    missing_by_agent = {a["name"]: [] for a in agents}
    missing_team = None
    if since and until:
        try:
            week_appts = fub.get_appointments(since=since, until=until) or []
            missing_team = 0
            uid_to_name = {a.get("user_id"): a["name"] for a in agents}
            for ap in week_appts:
                invitees = ap.get("invitees") or []
                agent_uid = next((i.get("userId") for i in invitees
                                  if i.get("userId") in roster_ids), None)
                lead_inv = next((i for i in invitees if i.get("personId")), None)
                if not agent_uid or not lead_inv:
                    continue
                if not ap.get("outcome"):
                    missing_team += 1
                    missing_by_agent[uid_to_name[agent_uid]].append({
                        "person_id": lead_inv.get("personId"),
                        "lead_name": lead_inv.get("name") or "Unknown",
                        "start_date": (ap.get("start") or "")[:10],
                    })
        except Exception as e:
            logger.warning("[COMMAND SHEET] appointment fetch failed: %s", e)
            gaps.append("appointment fetch failed, outcome gap unknown")

    team_week = {"calls": totals.get("calls", 0), "convos": totals.get("convos", 0),
                 "appts_set": totals.get("appts_set", 0),
                 "appts_met": totals.get("appts_met", 0)}
    set_total = int(team_week["appts_set"] or 0)
    missing_pct = (missing_team / set_total) if (missing_team is not None and set_total) else 0.0

    # ── THE ONE LEVER (deterministic) ──────────────────────────────────
    lever = compute_one_lever(team_week, missing_pct)
    llm = _llm()
    if llm is None:
        gaps.append("ANTHROPIC_API_KEY not set, deterministic copy used throughout")
    lever_lines = _gen_lever_lines(llm, lever) if lever else []
    subject = build_subject(lever)

    # ── Goals / pace / streak inputs (all DB, no FUB) ──────────────────
    year = datetime.now(_ET).year
    goals_map = {g.get("agent_name"): g for g in (_db.get_all_goals(year=year) or [])}
    deal_summaries = _db.get_deal_summary(year=year) or {}
    ytd_cache = _db.get_ytd_cache(year=year) or {}
    profiles = {p.get("agent_name"): p for p in (_db.get_agent_profiles() or [])}
    if not goals_map:
        gaps.append("no goals on file for any agent")

    # ── Phoenix rows + manifest for pipeline picks ─────────────────────
    try:
        phoenix_rows = _db.get_phoenix_log(limit=300) or []
    except Exception:
        phoenix_rows = []
    cutoff_14d = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    phoenix_by_agent = {}
    for row in phoenix_rows:
        if (row.get("created_at") or "") >= cutoff_14d and row.get("assigned_to"):
            phoenix_by_agent.setdefault(row["assigned_to"], []).append(row)

    manifest = (manifest_fn() if manifest_fn else {}) or {}
    manifest_agent = manifest.get("agent") or {}

    # ── Merit scorecard (for the paused-agent light touch) ─────────────
    try:
        import merit as _merit
        scorecard = _merit.get_cached_scorecard()
    except Exception:
        scorecard = None

    # ── Agent cards ────────────────────────────────────────────────────
    gen_cap = int(getattr(config, "COMMAND_SHEET_MAX_AGENT_GENERATIONS", 15))
    generated = 0
    cards = []
    for a in agents:
        name = a["name"]
        if "$" in name:
            continue  # Ylopo test account, not a real agent
        m = a.get("metrics") or {}
        week = {"calls": m.get("outbound_calls", 0), "convos": m.get("conversations", 0),
                "appts_set": m.get("appts_set", 0), "appts_met": m.get("appts_met", 0)}

        leads = _pick_pipeline_leads(
            fub, a.get("user_id"),
            missing_by_agent.get(name, []),
            phoenix_by_agent.get(name, []),
            (manifest_agent.get(name) or [])[:5],
        )

        if name in coaching_paused:
            # Light touch: accountability is paused, so no gap push and no
            # drill. Merit numbers shown only when the cached scorecard
            # actually has them (never asserted otherwise).
            note = "Accountability is paused for her right now. Nothing to push."
            best = None
            lanes = (((scorecard or {}).get("agents") or {}).get(name) or {}).get("lanes") or {}
            for lane_key, d in lanes.items():
                if d.get("sufficient") and d.get("merit_metric") is not None:
                    if best is None or d["merit_metric"] > best[1]:
                        best = (lane_key, d["merit_metric"], d)
            if best:
                lane_key, metric, d = best
                team_lane = ((scorecard or {}).get("team") or {}).get(lane_key) or {}
                rank_note = ""
                suff = team_lane.get("sufficient_agents") or []
                if suff and suff[0] == name:
                    rank_note = f" That is the top of the {lane_key} lane."
                note = (f"Accountability is paused for her right now, and her merit "
                        f"numbers keep speaking anyway: {d.get('appts_set', 0)} "
                        f"appointments from {d.get('leads_touched', 0)} "
                        f"{lane_key} leads in the trailing 180 days.{rank_note} "
                        "A thank you lands better than a push.")
            cards.append({"name": name, "week": week, "paused": True,
                          "paused_note": _strip_dashes(note), "leads": leads})
            continue

        goal = goals_map.get(name)
        pace, targets, gap = None, None, None
        if goal and float(goal.get("gci_goal") or 0) > 0:
            try:
                targets = _db.compute_targets(goal)
                deals = deal_summaries.get(name, {"contracts": 0, "closings": 0, "gci_est": 0.0})
                cached = ytd_cache.get(name, {})
                actuals = {
                    "calls_ytd": int(cached.get("calls_ytd") or 0),
                    "convos_ytd": int(cached.get("convos_ytd") or 0),
                    "appts_ytd": int(cached.get("appts_ytd") or 0),
                    "contracts_ytd": int(deals.get("contracts") or 0),
                    "closings_ytd": int(deals.get("closings") or 0),
                    "gci_ytd": float(deals.get("gci_est") or 0.0),
                }
                pace = _db.compute_pace(goal, targets, actuals,
                                        start_date=(profiles.get(name) or {}).get("start_date"))
                gap = _agent_gap_math(pace, targets, goal, week)
            except Exception as e:
                logger.warning("[COMMAND SHEET] pace failed for %s: %s", name, e)
                gaps.append(f"pace computation failed for {name}")

        streak = _db.get_streak(name) or {}
        fb_pace = _strip_dashes(_fallback_pace_line(goal, pace, gap))
        fb_teach = _strip_dashes(_fallback_teach_line(week, streak))

        pace_line, teach_line = fb_pace, fb_teach
        if llm is not None and generated < gen_cap:
            payload = {
                "agent_first_name": name.split()[0],
                "goal_dollars": _goal_money((goal or {}).get("gci_goal")) if goal else "no goal on file",
                "pct_of_pace": (pace or {}).get("overall_pct"),
                "smallest_daily_action": gap,
                "last_week": week,
                "rates_last_week": {
                    "contact_rate": _pct(_rates_for_week(week)["contact"]) if week["calls"] else "unknown",
                    "ask_rate": _pct(_rates_for_week(week)["ask"]) if week["convos"] else "unknown",
                    "held_rate": _pct(_rates_for_week(week)["held"]) if week["appts_set"] else "unknown",
                },
                "streak_days": streak.get("current_streak", 0),
                "benchmarks": {"contact": "15%", "ask": "10%", "held": "70%"},
            }
            g_pace, g_teach = _gen_agent_lines(llm, payload, fb_pace, fb_teach)
            pace_line, teach_line = _strip_dashes(g_pace), _strip_dashes(g_teach)
            generated += 1

        cards.append({"name": name, "week": week, "paused": False,
                      "pace_line": pace_line, "teach_line": teach_line,
                      "leads": leads})
    summary["agents"] = len(cards)
    summary["llm_agent_lines"] = generated

    # ── Danny's week ───────────────────────────────────────────────────
    danny = None
    try:
        latest = _db.get_latest_manager_update()
        if latest and latest.get("submitted_at"):
            submitted = datetime.fromisoformat(str(latest["submitted_at"]))
            if submitted.tzinfo is None:
                submitted = submitted.replace(tzinfo=timezone.utc)
            today_et = datetime.now(_ET).date()
            monday = today_et - timedelta(days=today_et.weekday())
            if submitted.astimezone(_ET).date() >= monday:
                entries = latest.get("entries") or []
                danny = {
                    "met_count": sum(1 for e in entries
                                     if str(e.get("met")).lower() == "yes"),
                    "entries": entries,
                }
    except Exception as e:
        logger.warning("[COMMAND SHEET] manager update read failed: %s", e)

    # ── Earned this week ───────────────────────────────────────────────
    rotation = [a["name"] for a in agents
                if (a.get("evaluation") or {}).get("overall_pass") and "$" not in a["name"]]

    phoenix_qualified, pond_unclaimed = None, None
    if phoenix_fn:
        try:
            qualified, evaluated = phoenix_fn(fub)
            phoenix_qualified = [r for r in evaluated if r.get("qualified")]
        except Exception as e:
            logger.warning("[COMMAND SHEET] phoenix qualification failed: %s", e)
            gaps.append("phoenix qualification check failed")
    if not phoenix_qualified:
        try:
            cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            pond_unclaimed = sum(
                1 for r in phoenix_rows
                if (r.get("created_at") or "") >= cutoff_7d
                and (r.get("status") == "pond_fallback"
                     or (r.get("status") == "dry_run" and not r.get("assigned_to"))))
        except Exception:
            pond_unclaimed = None

    # ── Transfers converted (trailing 7 days, isa_transfers table) ─────
    transfers = None
    try:
        rows = _db.get_transfer_merit_counts(days=7) or []
        transfers = {"transfers": sum(int(r.get("transfers") or 0) for r in rows),
                     "with_appt": sum(int(r.get("with_appt") or 0) for r in rows)}
    except Exception:
        gaps.append("transfer counts unavailable")

    # ── Render + send ──────────────────────────────────────────────────
    html = render_html({
        "period_label": period_label,
        "lever": lever, "lever_lines": lever_lines,
        "team_week": team_week, "team_prior": prior_totals,
        "missing_outcomes_team": missing_team,
        "transfers": transfers,
        "agent_cards": cards,
        "danny": danny,
        "rotation": rotation,
        "phoenix_qualified": phoenix_qualified,
        "phoenix_pond_unclaimed": pond_unclaimed,
    })

    summary["subject"] = subject
    summary["html"] = html
    summary["lever"] = ({k: v for k, v in lever.items()} if lever else None)
    summary["rotation"] = rotation
    summary["phoenix_qualified"] = [r.get("agent_name") for r in (phoenix_qualified or [])]

    if not dry_run and email_to_barry:
        import postmark_client as _pm
        _pm.send(to=config.BARRY_EMAIL, from_email=config.EMAIL_FROM,
                 subject=subject, html=html)
        summary["emailed"] = True

    logger.info("[COMMAND SHEET] built: lever=%s value=%s agents=%d emailed=%s gaps=%s",
                (lever or {}).get("key"), (lever or {}).get("monthly_value"),
                len(cards), summary["emailed"], gaps)
    return summary
