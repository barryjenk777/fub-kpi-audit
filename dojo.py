"""
dojo.py — The Dojo: weekly role-play training + accountability engine.

The insight: Maverick's practice bots grade calls 1-10 and the nightly
courier already harvests every agent's REAL call grades (avg grade, appt
ask %, objection handling). So training can finally work like a great
human sales manager runs it:

  DIAGNOSE  — each agent's weakest skill, from their own graded calls
  PRESCRIBE — the exact bot that trains that weakness, a rep count sized
              to the gap, and a passing bar (7+)
  VERIFY    — next week's harvest shows whether the real numbers moved;
              the prescription tightens or graduates accordingly

Monday morning email per agent (their numbers, their assignment, one
teaching beat in Barry's Too Nice for Sales voice), a /training board for
Barry and Danny, and the Wednesday digest nudges the reps.
"""

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import config
import db as _db

logger = logging.getLogger(__name__)

_EXCLUDED = set(getattr(config, "EXCLUDED_USERS", [])) \
    | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))
PASS = getattr(config, "DOJO_PASSING_GRADE", 7)


def _first(name):
    return (name or "").split()[0] if name else "there"


def _pretty(phone):
    d = "".join(c for c in (phone or "") if c.isdigit())[-10:]
    return "(%s) %s-%s" % (d[:3], d[3:6], d[6:]) if len(d) == 10 else phone


def _scenario(pool_name, difficulty_idx):
    pool = getattr(config, pool_name, []) or []
    if not pool:
        return None
    return pool[min(difficulty_idx, len(pool) - 1)]


def diagnose(agent_name, mine, team, iso_week):
    """Deterministic weekly diagnosis from the agent's own graded calls.
    Returns dict(focus, reason, scenarios[list], reps)."""
    side_pool = ("MAVERICK_SELLER_SCENARIOS" if iso_week % 2
                 else "MAVERICK_BUYER_SCENARIOS")
    graded = int(mine.get("calls_graded") or 0)
    ask = mine.get("appt_ask")
    obj = mine.get("objection")
    grade = mine.get("avg_grade")
    team_ask = (team or {}).get("appt_ask")

    if graded < 2:
        return {
            "focus": "get_on_the_board",
            "reason": ("Maverick graded %d of your calls last week. Not enough "
                       "real conversations to coach from yet, so this week the "
                       "reps come first." % graded),
            "scenarios": [_scenario(side_pool, 0)],
            "reps": 3,
        }
    if ask is not None and ask < 50:
        sev = 4 if ask < 30 else 3
        return {
            "focus": "the_ask",
            "reason": ("You asked for the appointment on %d%% of your graded "
                       "calls%s. The conversation is happening; the ask is not."
                       % (round(ask),
                          (" (team average %d%%)" % round(team_ask)) if team_ask else "")),
            "scenarios": [_scenario(side_pool, 3)],   # Disinterested: forces the ask
            "reps": sev,
        }
    if obj is not None and obj < 60:
        return {
            "focus": "objections",
            "reason": ("Maverick scored your objection handling at %d%%. When "
                       "they push back, the call is ending instead of turning."
                       % round(obj)),
            "scenarios": [_scenario(side_pool, 1)],   # Irate: pure objection reps
            "reps": 3,
        }
    if grade is not None and grade < 6:
        return {
            "focus": "fundamentals",
            "reason": ("Your graded calls average %.1f out of 10. Nothing broken, "
                       "everything a notch soft. Standard reps sharpen all of it."
                       % grade),
            "scenarios": [_scenario(side_pool, 0)],
            "reps": 3,
        }
    # Passing everywhere: advanced polish, rotating specialty scenarios
    adv_pools = ["MAVERICK_CASH_OFFER_SCENARIOS", "MAVERICK_SELLER_BUYER_SCENARIOS",
                 side_pool]
    pool = adv_pools[iso_week % len(adv_pools)]
    return {
        "focus": "sharpen",
        "reason": ("Your numbers clear the bar (%.1f average, %d%% ask rate). "
                   "This week is edge work: a harder scenario to stay sharp."
                   % (grade or 0, round(ask or 0))),
        "scenarios": [_scenario(pool, min(2, iso_week % 4))],
        "reps": 2,
    }


_TEACH_FALLBACK = {
    "get_on_the_board": ("Reps beat rust. Two minutes with a bot costs you "
                         "nothing and makes the next real dial feel routine."),
    "the_ask": ("Too nice to your comfort zone is the trap here. Asking for the "
                "appointment is not pressure, it is service: the person on the "
                "phone cannot buy or sell a house through a phone call. The "
                "meeting is where their problem actually gets solved. Practice "
                "saying it until it stops feeling like an imposition."),
    "objections": ("An objection is not a wall, it is the lead telling you what "
                   "matters to them. The rep goal is simple: when the bot pushes "
                   "back, get curious instead of defensive. One question, then "
                   "silence."),
    "fundamentals": ("Every great call has the same skeleton: connect, discover, "
                     "prescribe, book. The bot will wander; your job is to keep "
                     "the skeleton under the conversation."),
    "sharpen": ("Mastery is maintained, not achieved. The harder scenario is "
                "there so real calls feel easy by comparison."),
}


def _teach_line(focus, reason):
    """One teaching paragraph grounded in the Too Nice for Sales manuscript
    (dojo_teachings bank). LLM composes from Barry's own principles; the
    verbatim book quote and chapter reference ride along either way."""
    try:
        from dojo_teachings import TEACHINGS
        bank = TEACHINGS.get(focus) or {}
    except Exception:
        bank = {}
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key and bank:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            resp = client.messages.create(
                model="claude-haiku-4-5", max_tokens=260,
                system=("You write ONE short teaching paragraph (3-4 sentences) "
                        "as Barry Jenkins coaching his own agent, drawing ONLY "
                        "on the supplied principles from his book Too Nice for "
                        "Sales. Voice: warm, conversational, teaching over "
                        "pushing, never shaming, the skill reframed as service "
                        "to the client. You may paraphrase the principles; do "
                        "not invent new claims, numbers, or stories. No em or "
                        "en dashes. No greeting or sign-off."),
                messages=[{"role": "user", "content":
                           "Weakness this week: %s.\nAgent context: %s\n\n"
                           "Principles from the book (%s):\n- %s"
                           % (focus.replace('_', ' '), reason,
                              bank.get("chapters", ""),
                              "\n- ".join(bank.get("principles", [])))}],
                extra_body={"temperature": 0.5},
            )
            text = resp.content[0].text.strip()
            for dash in ("—", "–", " - "):
                text = text.replace(dash, ", ")
            if 60 < len(text) < 700:
                return text
        except Exception as e:
            logger.warning("dojo teach line LLM failed: %s", e)
    if bank.get("principles"):
        return bank["principles"][0]
    return _TEACH_FALLBACK.get(focus, _TEACH_FALLBACK["fundamentals"])


def build_email(agent_name, diag, mine, team):
    first = _first(agent_name)
    sc = diag["scenarios"][0] or {}
    teach = _teach_line(diag["focus"], diag["reason"])
    quote_html = ""
    try:
        from dojo_teachings import TEACHINGS
        bank = TEACHINGS.get(diag["focus"]) or {}
        if bank.get("quote"):
            quote_html = ("<div style='border-left:3px solid #b97a12;padding:"
                          ".4em .9em;margin:.6em 0;color:#5b6779;font-size:14px;"
                          "font-style:italic'>\"%s\"<div style='font-style:normal;"
                          "font-size:12px;margin-top:.3em'>Too Nice for Sales, %s"
                          "</div></div>" % (bank["quote"], bank.get("chapters", "")))
    except Exception:
        pass
    graded = int(mine.get("calls_graded") or 0)
    stats_bits = []
    if graded:
        if mine.get("avg_grade") is not None:
            stats_bits.append("Average grade: <b>%.1f / 10</b>" % mine["avg_grade"])
        if mine.get("appt_ask") is not None:
            stats_bits.append("Asked for the appointment: <b>%d%%</b>%s" % (
                round(mine["appt_ask"]),
                (" (team %d%%)" % round(team["appt_ask"])) if team.get("appt_ask") else ""))
        if mine.get("objection") is not None:
            stats_bits.append("Objection handling: <b>%d%%</b>" % round(mine["objection"]))
    stats_html = ("<ul style='margin:.4em 0 0 1.2em;padding:0'>" +
                  "".join("<li style='margin:.2em 0'>%s</li>" % b for b in stats_bits) +
                  "</ul>") if stats_bits else ""

    subject = "Your training this week, %s" % first
    html = f"""
<div style='font-family:-apple-system,Segoe UI,sans-serif;max-width:560px;margin:0 auto;color:#1a2233;line-height:1.55'>
<p style='font-size:15px'>Morning {first}. Maverick graded {graded} of your real calls last week. Here is what the tape says:</p>
<div style='background:#f8fafc;border:1px solid #e6eaf1;border-radius:10px;padding:12px 16px;font-size:14px'>
{diag['reason']}{stats_html}
</div>
<p style='font-size:15px'>{teach}</p>
{quote_html}
<div style='background:#fdf6ea;border:1px solid #f0dfc0;border-radius:10px;padding:14px 16px'>
  <div style='font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#b97a12'>This week's reps</div>
  <div style='font-size:16px;font-weight:700;margin:.3em 0'>{sc.get('label','Practice call')} · {diag['reps']} calls</div>
  <div style='font-size:14px'>Call <a href='tel:{sc.get('phone','')}' style='color:#b97a12;font-weight:700'>{_pretty(sc.get('phone',''))}</a>. Who picks up: {sc.get('label','a practice lead')}, {sc.get('context','ready to test you')}.</div>
  <div style='font-size:13px;color:#5b6779;margin-top:.4em'>A rep counts when Maverick grades it <b>{PASS} or higher</b>. It emails you the score right after each call. Under {PASS}? Read the feedback, call again.</div>
</div>
<p style='font-size:14px'>Two minutes a rep. Nobody real on the line, nothing to lose, and next week's tape is how we both know it worked. Danny and I see the board.</p>
<p style='font-size:14px'>Barry</p>
</div>"""
    return subject, html


def run_dojo_monday(dry_run=False):
    """Diagnose every agent and send the Monday training email."""
    from nudge_engine import AGENT_EMAIL_OVERRIDES
    import postmark_client as _pm

    stats = _db.get_latest_maverick_stats()
    team = stats.get("Team Average") or {}
    today = date.today()
    iso_week = today.isocalendar()[1]
    week_start = today - timedelta(days=today.weekday())
    summary = {"sent": 0, "skipped": 0, "emails": []}

    for profile in (_db.get_agent_profiles(active_only=True) or []):
        agent = profile.get("agent_name")
        if not agent or agent in _EXCLUDED:
            continue
        email = AGENT_EMAIL_OVERRIDES.get(agent) or profile.get("email")
        if not email:
            summary["skipped"] += 1
            continue
        mine = stats.get(agent) or {}
        diag = diagnose(agent, mine, team, iso_week)
        sc = diag["scenarios"][0] or {}
        subject, html = build_email(agent, diag, mine, team)
        summary["emails"].append({"agent": agent, "focus": diag["focus"],
                                  "reps": diag["reps"],
                                  "scenario": sc.get("label"),
                                  "subject": subject, "html": html})
        if dry_run:
            continue
        if not _db.claim_once("dojo_%s_%s" % (week_start.isoformat(), agent)):
            continue
        try:
            _pm.send(to=email, from_email=config.EMAIL_FROM,
                     subject=subject, html=html)
            _db.save_dojo_prescription(week_start, agent, diag["focus"],
                                       sc.get("label"), sc.get("phone"),
                                       diag["reps"], diag["reason"])
            summary["sent"] += 1
        except Exception as e:
            logger.error("dojo email failed for %s: %s", agent, e)
    return summary
