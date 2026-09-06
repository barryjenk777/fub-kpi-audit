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
    graded = int(mine.get("calls_graded") or 0)

    subject = "Your training this week, %s" % first

    focus_titles = {
        "the_ask": "The Ask", "objections": "Objections",
        "fundamentals": "Fundamentals", "get_on_the_board": "Get On The Board",
        "sharpen": "Stay Sharp",
    }
    focus_title = focus_titles.get(diag["focus"], "Training")
    base = os.environ.get("BASE_URL",
                          "https://web-production-3363cc.up.railway.app").rstrip("/")

    # Book quote in the house quote-block style (goal email pattern)
    quote_block = ""
    try:
        from dojo_teachings import TEACHINGS
        bank = TEACHINGS.get(diag["focus"]) or {}
        if bank.get("quote"):
            quote_block = f"""
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 24px">
          <tr>
            <td style="border-left:4px solid #f5a623;padding:14px 20px;background:#fffbf0;border-radius:0 8px 8px 0">
              <p style="margin:0;font-size:15px;font-style:italic;color:#555555;line-height:1.6">"{bank['quote']}"</p>
              <p style="margin:8px 0 0;font-size:12px;font-weight:700;color:#f5a623;letter-spacing:0.5px">TOO NICE FOR SALES, {bank.get('chapters', '').upper()}</p>
            </td>
          </tr>
        </table>"""
    except Exception:
        pass

    # Stat tiles in house palette
    tiles = []
    if graded:
        if mine.get("avg_grade") is not None:
            tiles.append(("%.1f" % mine["avg_grade"], "AVG CALL GRADE", "of 10"))
        if mine.get("appt_ask") is not None:
            tiles.append(("%d%%" % round(mine["appt_ask"]), "ASKED FOR THE APPT",
                          ("team %d%%" % round(team["appt_ask"])) if team.get("appt_ask") else ""))
        if mine.get("objection") is not None:
            tiles.append(("%d%%" % round(mine["objection"]), "OBJECTIONS HANDLED", ""))
    tiles_html = ""
    if tiles:
        cells = "".join(f"""
            <td width="{100 // len(tiles)}%" style="padding:4px">
              <table width="100%" cellpadding="0" cellspacing="0"
                     style="background:#fffbf0;border:1px solid #f5e3bb;border-radius:8px">
                <tr><td style="padding:14px 8px;text-align:center">
                  <p style="margin:0;font-size:26px;font-weight:800;color:#111111;line-height:1">{v}</p>
                  <p style="margin:6px 0 0;font-size:10px;font-weight:700;letter-spacing:0.5px;color:#888888">{l}</p>
                  {f'<p style="margin:2px 0 0;font-size:11px;font-weight:700;color:#f5a623">{s}</p>' if s else ''}
                </td></tr></table></td>"""
            for v, l, s in tiles)
        tiles_html = (f'<table width="100%" cellpadding="0" cellspacing="0" '
                      f'style="margin:0 0 24px"><tr>{cells}</tr></table>')

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f0;padding:32px 16px">
  <tr><td align="center">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:560px">

    <!-- Header bar -->
    <tr>
      <td style="background:#0d1117;border-radius:12px 12px 0 0;padding:24px 32px;text-align:center">
        <img src="{base}/static/logo-white.png"
             alt="Legacy Home Team" width="160" style="display:block;margin:0 auto 10px;width:160px;height:auto">
        <p style="margin:0 0 4px;font-size:11px;font-weight:700;letter-spacing:2px;color:#f5a623">THE DOJO &middot; WEEKLY TRAINING</p>
        <p style="margin:0;font-size:20px;font-weight:800;color:#ffffff">This Week: {focus_title}</p>
      </td>
    </tr>

    <!-- Body -->
    <tr>
      <td style="background:#ffffff;padding:36px 32px 28px;border-left:1px solid #e5e5e5;border-right:1px solid #e5e5e5">

        <p style="margin:0 0 20px;font-size:16px;color:#111111">Hey {first},</p>

        <p style="margin:0 0 16px;font-size:15px;line-height:1.65;color:#333333">
          Maverick graded <strong style="color:#111111">{graded}</strong> of your real
          calls last week. Here's what the tape says:</p>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 20px">
          <tr>
            <td style="border-left:4px solid #f5a623;padding:14px 20px;background:#fffbf0;border-radius:0 8px 8px 0">
              <p style="margin:0;font-size:15px;color:#333333;line-height:1.6">{diag['reason']}</p>
            </td>
          </tr>
        </table>

        {tiles_html}

        <p style="margin:0 0 16px;font-size:15px;line-height:1.65;color:#333333">{teach}</p>

        {quote_block}

        <!-- Assignment card -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 28px">
          <tr>
            <td style="background:#0d1117;border-radius:12px;padding:24px 28px;text-align:center">
              <p style="margin:0 0 4px;font-size:11px;font-weight:700;letter-spacing:2px;color:#f5a623">THIS WEEK'S REPS</p>
              <p style="margin:0 0 6px;font-size:22px;font-weight:800;color:#ffffff">{sc.get('label','Practice call')} &times; {diag['reps']}</p>
              <p style="margin:0 0 18px;font-size:13px;color:#aab2c0;line-height:1.5">Who picks up: {sc.get('context','a practice lead ready to test you')}.</p>
              <a href="tel:{sc.get('phone','')}"
                 style="display:inline-block;background:#f5a623;color:#0d1117;padding:16px 36px;
                        border-radius:8px;text-decoration:none;font-weight:800;font-size:16px;
                        letter-spacing:0.3px">
                Call the Bot: {_pretty(sc.get('phone',''))} &rarr;
              </a>
              <p style="margin:16px 0 0;font-size:12px;color:#aab2c0;line-height:1.6">
                A rep counts at <strong style="color:#f5a623">{PASS}+</strong>.
                Maverick emails your score right after each call.<br>
                Under {PASS}? Read the feedback, call again.</p>
            </td>
          </tr>
        </table>

        <p style="margin:0 0 16px;font-size:15px;line-height:1.65;color:#333333">
          Two minutes a rep. Nobody real on the line, nothing to lose, and next
          week's tape is how we both know it worked. Wednesday's text shows your
          rep count. Danny and I see the board.</p>

        <p style="margin:24px 0 0;font-size:15px;color:#111111">
          Let's get it,<br>
          <strong>Barry Jenkins</strong><br>
          <span style="font-size:13px;color:#888888">Legacy Home Team</span>
        </p>
      </td>
    </tr>

    <!-- Footer -->
    <tr>
      <td style="background:#0d1117;border-radius:0 0 12px 12px;padding:16px 32px;text-align:center">
        <p style="margin:0;font-size:11px;color:#666f7d">Built from your own graded calls &middot; refreshed nightly &middot; Legacy Home Team</p>
      </td>
    </tr>

  </table>
  </td></tr>
</table>
</body>
</html>"""
    return subject, html


def run_dojo_monday(dry_run=False):
    """Diagnose every agent and send the Monday training email."""
    from nudge_engine import AGENT_EMAIL_OVERRIDES
    import postmark_client as _pm

    stats = _db.get_latest_maverick_stats()
    team = stats.get("Team Average") or {}
    today = date.today()
    # Sent Sunday night for the COMING week: week_start = the next Monday
    # (or today when run on a Monday), so Wednesday's digest rep-check finds
    # the right prescription row.
    week_start = today + timedelta(days=(7 - today.weekday()) % 7)
    iso_week = week_start.isocalendar()[1]
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
