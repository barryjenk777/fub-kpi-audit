"""
Legacy Home Team — Nudge Engine (Phase 3, Email Edition)

Sends personalized email nudges to agents based on:
- Morning power hour (daily, at their chosen time)
- Missed day check (5pm if nothing logged yet)
- Streak break (next morning after a missed day)
- Weekly summary (Sunday evening)
- Milestone celebrations (first close, streak milestones)
- Post-closing follow-up reminders (30/60/90 days)

All messages pull from the agent's stored 'why', 'identity', and activity data.
Uses a 15-20 message template pool so emails never feel repetitive.

Environment variables required:
  SENDGRID_API_KEY
  EMAIL_FROM      (e.g. barry@legacyhometeam.com)
  BASE_URL        (for dashboard links)
"""

import logging
import os
import random
from datetime import date, datetime, timezone, timedelta

import db as _db

logger = logging.getLogger(__name__)

BASE_URL  = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")
LOGO_URL  = f"{BASE_URL}/static/logo-white.png"
EMAIL_FROM = os.environ.get("EMAIL_FROM", "barry@legacyhometeam.com")


# ---------------------------------------------------------------------------
# Email send
# ---------------------------------------------------------------------------

def _send_email(to_email: str, subject: str, text_body: str,
                dashboard_url: str = "", dry_run: bool = False) -> dict:
    """Send a nudge email via SendGrid. Returns {status} or raises."""
    if dry_run:
        logger.info("[DRY RUN] Email to %s | %s", to_email, subject)
        return {"status": "dry_run"}

    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY not set")

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail
    except ImportError:
        raise RuntimeError("sendgrid package not installed")

    # Dashboard button (optional, shown on missed_day / streak_break)
    dashboard_btn = ""
    if dashboard_url:
        dashboard_btn = f"""
    <div style="text-align:center;margin:24px 0 8px">
      <a href="{dashboard_url}"
         style="display:inline-block;background:#f5a623;color:#0d1117;padding:13px 32px;
                border-radius:8px;text-decoration:none;font-weight:800;font-size:14px">
        Log My Numbers →
      </a>
    </div>"""

    # Render message text with line breaks preserved
    html_text = text_body.replace("\n", "<br>")

    html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<div style="max-width:520px;margin:24px auto;background:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
  <div style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:22px 32px;text-align:center">
    <img src="{LOGO_URL}" alt="Legacy Home Team" width="120" style="display:block;margin:0 auto;height:auto">
  </div>
  <div style="padding:28px 32px">
    <p style="font-size:16px;line-height:1.75;color:#2d3748;margin:0 0 12px">{html_text}</p>
    {dashboard_btn}
  </div>
  <div style="background:#f7fafc;padding:14px 32px;text-align:center;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:12px;color:#a0aec0">
      Legacy Home Team &middot; Your daily accountability system<br>
      <a href="{dashboard_url or BASE_URL}" style="color:#a0aec0">View My Dashboard</a>
    </p>
  </div>
</div>
</body></html>"""

    msg = Mail(
        from_email=EMAIL_FROM,
        to_emails=to_email,
        subject=subject,
        plain_text_content=text_body,
        html_content=html_body,
    )

    sg = SendGridAPIClient(api_key)
    resp = sg.send(msg)
    return {"status": "sent", "code": resp.status_code}


# ---------------------------------------------------------------------------
# Agent context helper
# ---------------------------------------------------------------------------

def _ctx(agent_name: str) -> dict:
    """Build a context dict for personalizing messages."""
    first  = agent_name.split()[0] if agent_name else "there"
    why    = _db.get_agent_why(agent_name) or {}
    ident  = _db.get_agent_identity(agent_name) or {}
    streak = _db.get_streak(agent_name) or {}
    goals  = _db.get_goal(agent_name, year=date.today().year) or {}

    who = who_label(why.get("who_benefits", ""), why.get("who_benefits_custom", ""))
    archetype_labels = {
        "consistent": "The Consistent One",
        "closer": "The Closer",
        "prospecting_machine": "The Prospecting Machine",
        "relationship_builder": "The Relationship Builder",
        "comeback_story": "The Comeback Story",
    }
    identity_label = (
        ident.get("custom_identity")
        or archetype_labels.get(ident.get("identity_archetype", ""), "a top producer")
    )

    daily_calls = ident.get("daily_calls_target", 20)
    gci_goal    = goals.get("gci_goal", 0)

    # Dashboard URL
    token = _db.get_goal_token(agent_name) or ""
    dashboard_url = f"{BASE_URL}/my-goals/{token}" if token else ""

    return {
        "first": first,
        "who": who,
        "what_happens": why.get("what_happens", "your goals"),
        "why": why.get("why_statement", ""),
        "identity": identity_label,
        "streak": streak.get("current_streak", 0),
        "longest": streak.get("longest_streak", 0),
        "daily_calls": daily_calls,
        "gci_goal": gci_goal,
        "gci_fmt": _fmt_gci(gci_goal),
        "dashboard_url": dashboard_url,
    }


def who_label(who_benefits: str, custom: str) -> str:
    labels = {
        "my_kids": "your kids",
        "spouse_partner": "your spouse",
        "myself": "you",
        "my_family": "your family",
    }
    return custom or labels.get(who_benefits, "the people you love")


def _fmt_gci(n):
    if not n: return "your goal"
    n = float(n)
    if n >= 1000000: return f"${n/1000000:.1f}M"
    if n >= 1000:    return f"${round(n/1000)}K"
    return f"${round(n)}"


# ---------------------------------------------------------------------------
# Message template pools
# ---------------------------------------------------------------------------

MORNING_TEMPLATES = [
    "{first}, {who} gets closer with every dial. {daily_calls} calls today. Let's go.",
    "You said you wanted to be {identity}. That agent makes their calls before 10am. Today's your day.",
    "Day {streak} on the streak, {first}. {who} would be proud of this consistency. Don't stop.",
    "{first} — {daily_calls} calls, 5 texts, 1 appointment. That's the job today.",
    "The agents who close 40+ deals this year aren't smarter. They just show up every day. Today's your day, {first}.",
    "Every call is a step toward: '{what_happens}'. Make {daily_calls} of them today, {first}.",
    "{first}, your power hour starts now. {daily_calls} dials. Everything else can wait.",
    "What would {identity} do this morning? Make the calls. Log the numbers. Stack the day.",
    "{first} — one hour of focused calls today = {daily_calls} chances to change your year.",
    "The compound effect is real. {daily_calls} calls today + every day = {gci_fmt}. Let's go.",
    "Doors open for {who} when you pick up the phone. {daily_calls} calls today, {first}.",
    "You built this far one call at a time. Today's another brick. {daily_calls} calls, {first}.",
    "Remember why you started, {first}: {why_short}. That's worth {daily_calls} calls.",
    "No motivation today? Good. Motivation is overrated. {identity} shows up anyway. Call.",
    # Inspirational — mindset
    "Most agents will scroll their phone this morning. You're going to dial it. That's the difference, {first}.",
    "{first}, the gap between where you are and {gci_fmt} is just reps. Start the reps today.",
    "Somewhere in your pipeline right now is someone ready to buy or sell. They're waiting for your call. Make {daily_calls} today.",
    "Real estate isn't hard — it just requires doing the uncomfortable thing (calling) every single day. You've got this, {first}.",
    "The best time to call was yesterday. The second best time is right now. {daily_calls} calls, {first}. Go.",
    "{first}, champions don't wait to feel ready. They make the call, then feel ready. {daily_calls} today.",
    "One conversation can change everything. You need {daily_calls} chances to find it today. Pick up the phone.",
    "You're not just making calls, {first}. You're building the life {who} deserves. {daily_calls} today.",
    "Every top producer on this team started with one call. Then another. Then another. Your turn, {first}.",
    "The market doesn't care about your mood. Your goals don't either. {daily_calls} calls. Let's build.",
    # Streak-aware
    "Streak: {streak} days. You're proving something to yourself every single morning, {first}. Keep proving it.",
    "{first}, you've shown up {streak} days straight. Today is day {streak_plus}. Make it count.",
    # Closer-to-goal feel
    "Every dial today is a step toward '{what_happens}'. {daily_calls} steps, {first}. Start.",
    "{first}, {gci_fmt} this year isn't a dream — it's a daily decision. Today's decision is {daily_calls} calls.",
]

MISSED_DAY_TEMPLATES = [
    "{first}, haven't seen your numbers today. Even 5 calls keeps the chain alive. Under 30 seconds to log.",
    "Quick check-in — your activity isn't in yet. One call. Log it. That's the two-minute version.",
    "{first} — the day isn't over yet. {daily_calls} calls feels like a lot. Try 5. Then keep going.",
    "Still time today, {first}. Open the app, make one call, log it. The streak is worth saving.",
    "{first}, your streak is still alive until midnight. Don't let today be the day it ends.",
]

STREAK_BREAK_TEMPLATES = [
    "Streak reset to 0. That's fine — the best agents aren't perfect, they're persistent. New streak starts with one call today, {first}.",
    "{first}, missed yesterday. That's done now. Today is day 1 of the next streak. {who} is still waiting.",
    "Every athlete misses a practice. What matters is the next one. That's today, {first}. Let's go.",
    "The valley of disappointment is real — effort builds before results show. Don't stop now, {first}. New streak today.",
    "{first}, the chain broke. Rebuild it. One call. One log. Day 1.",
]

WEEKLY_SUMMARY_TEMPLATES = [
    "{first}, this week: {calls} calls, {appts} appointments. You're {pace_word} pace for {gci_fmt}. {one_liner}",
    "Week in review, {first}: {calls} calls. {appts} appointments. Streak: {streak} days. {one_liner}",
    "{first} — your week: {calls} calls, {appts} appts. {pace_word} your {gci_fmt} goal. Next week, let's top it.",
]

MILESTONE_TEMPLATES = {
    "first_close": "FIRST ONE DOWN, {first}! {remaining} closings to go to hit {gci_fmt}. {who} is getting closer. 🏡",
    "streak_7":    "7 days straight, {first}. That's a real streak. Science says you're forming a habit. Keep going. 🔥",
    "streak_14":   "14 days straight. Two solid weeks, {first}. This is what {identity} looks like. 🔥🔥",
    "streak_21":   "21 days straight, {first}. Science says that's a habit now. You're becoming {identity}. 🏆",
    "streak_30":   "30 DAYS. {first}, that is elite consistency. {who} should see this. 💪",
    "pace_ahead":  "{first}, you're ahead of pace for {gci_fmt} this month. The compound effect is real.",
    "post_closing":"You just closed. In 30 days, reach out to {client} for a referral — they're warm. I'll remind you.",
}

# Subject lines per nudge type
SUBJECTS = {
    "morning":        "Time to make your calls, {first} 🔥",
    "missed_day":     "{first}, your streak is still alive — log before midnight",
    "streak_break":   "{first} — reset, restart, go. Day 1 starts now.",
    "weekly_summary": "Your week in numbers, {first}",
    "plateau":        "The compound effect is building, {first}",
    "milestone_first_close": "First closing! 🏡 {first}, you're on the board",
    "milestone_streak_7":    "7-day streak 🔥 {first}, you're building a habit",
    "milestone_streak_14":   "14 days straight 🔥🔥 {first}, this is momentum",
    "milestone_streak_21":   "21-day streak 🏆 {first}, it's officially a habit",
    "milestone_streak_30":   "30-DAY STREAK 💪 {first}, that's elite",
    "milestone_pace_ahead":  "Ahead of pace, {first} — keep going",
    "post_closing":          "30-day follow-up reminder, {first}",
}


def _pick(templates, ctx):
    """Pick a random template and format it with ctx, with safe fallback."""
    tmpl = random.choice(templates)
    try:
        why_short = (ctx.get("why") or "")[:60]
        if len(ctx.get("why") or "") > 60: why_short += "…"
        return tmpl.format(
            first=ctx["first"],
            who=ctx["who"],
            what_happens=ctx["what_happens"],
            why=ctx["why"],
            why_short=why_short,
            identity=ctx["identity"],
            streak=ctx["streak"],
            streak_plus=ctx["streak"] + 1,
            longest=ctx["longest"],
            daily_calls=ctx["daily_calls"],
            gci_fmt=ctx["gci_fmt"],
            gci_goal=ctx["gci_goal"],
        )
    except KeyError:
        return tmpl  # Return unformatted rather than crash


def _subject(nudge_type: str, ctx: dict) -> str:
    tmpl = SUBJECTS.get(nudge_type, "A message from Legacy Home Team")
    try:
        return tmpl.format(**ctx)
    except Exception:
        return tmpl


# ---------------------------------------------------------------------------
# Nudge dispatcher
# ---------------------------------------------------------------------------

def nudge_agent(agent_name: str, nudge_type: str, email: str,
                extra: dict = None, dry_run: bool = False) -> bool:
    """
    Build and send an email nudge of the given type.

    nudge_type: 'morning' | 'missed_day' | 'streak_break' | 'weekly_summary' |
                'milestone_*' | 'custom'
    extra:  additional merge fields (calls, appts, remaining, client, pace_word, one_liner)
    Returns True on success.
    """
    if not email:
        logger.warning("nudge_agent: no email for %s", agent_name)
        return False

    ctx = _ctx(agent_name)
    if extra:
        ctx.update(extra)

    # Guard: don't send same type twice today (except custom)
    if nudge_type != "custom":
        counts = _db.get_nudge_counts_today(agent_name)
        if counts.get(nudge_type, 0) > 0:
            logger.info("Skipping %s nudge for %s — already sent today", nudge_type, agent_name)
            return False

    # Build message body
    if nudge_type == "morning":
        msg = _pick(MORNING_TEMPLATES, ctx)
    elif nudge_type == "missed_day":
        msg = _pick(MISSED_DAY_TEMPLATES, ctx)
    elif nudge_type == "streak_break":
        msg = _pick(STREAK_BREAK_TEMPLATES, ctx)
    elif nudge_type == "weekly_summary":
        weekly_tmpl = random.choice(WEEKLY_SUMMARY_TEMPLATES)
        pace_word = ctx.get("pace_word", "on")
        one_liner = ctx.get("one_liner", "Keep building.")
        try:
            msg = weekly_tmpl.format(
                first=ctx["first"],
                calls=ctx.get("calls", 0),
                appts=ctx.get("appts", 0),
                streak=ctx["streak"],
                gci_fmt=ctx["gci_fmt"],
                pace_word=pace_word,
                one_liner=one_liner,
            )
        except Exception:
            msg = f"Hey {ctx['first']}, check your dashboard for this week's numbers. Keep going."
    elif nudge_type.startswith("milestone_"):
        key = nudge_type.replace("milestone_", "")
        tmpl = MILESTONE_TEMPLATES.get(key, "Great work, {first}! Keep it up.")
        try:
            msg = tmpl.format(**ctx)
        except Exception:
            msg = f"Great milestone, {ctx['first']}! Keep going."
    elif nudge_type == "custom":
        msg = extra.get("message", f"Hey {ctx['first']}, checking in. Keep up the great work!")
    else:
        logger.warning("Unknown nudge_type: %s", nudge_type)
        return False

    subject = _subject(nudge_type, ctx)

    # Include dashboard link button on actionable nudges
    dashboard_url = ctx.get("dashboard_url", "")
    show_btn = nudge_type in ("missed_day", "streak_break")

    try:
        result = _send_email(
            email, subject, msg,
            dashboard_url=dashboard_url if show_btn else "",
            dry_run=dry_run,
        )
        _db.log_nudge(agent_name, nudge_type, msg, status=result.get("status", "sent"))
        logger.info("Nudge email sent [%s] → %s: %s", nudge_type, agent_name, msg[:60])
        return True
    except Exception as e:
        _db.log_nudge(agent_name, nudge_type, msg, status="failed")
        logger.warning("nudge_agent email failed for %s: %s", agent_name, e)
        return False


# ---------------------------------------------------------------------------
# Scheduled job runners (called by APScheduler in app.py)
# ---------------------------------------------------------------------------

def run_morning_nudges(dry_run: bool = False):
    """
    Called once daily at 8am ET.
    Pulls yesterday's FUB-synced activity for all agents, ranks the team,
    then sends each agent a personalized sassy email with their rank,
    team context, and top 3 LeadStream leads.
    """
    yesterday = date.today() - timedelta(days=1)
    day_name  = yesterday.strftime("%A")

    # Single query — all agents' yesterday FUB data
    team_data = _db.get_team_activity_yesterday()
    if not team_data:
        logger.warning("run_morning_nudges: no team data found")
        return 0

    # Build leaderboard (calls + texts + appts*3)
    leaderboard = []
    for name, d in team_data.items():
        calls = int(d.get("calls", 0) or 0)
        appts = int(d.get("appts", 0) or 0)
        texts = int(d.get("texts", 0) or 0)
        score = calls + texts + (appts * 3)
        leaderboard.append({
            "name": name, "email": d["email"],
            "calls": calls, "appts": appts, "texts": texts, "score": score,
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    team_size      = len(leaderboard)
    team_avg_calls = round(sum(a["calls"] for a in leaderboard) / max(team_size, 1))
    top_agent      = leaderboard[0]

    sent = 0
    for rank, agent in enumerate(leaderboard, 1):
        name  = agent["name"]
        email = agent["email"]
        if not email:
            continue
        if _db.get_nudge_counts_today(name).get("morning", 0) > 0:
            continue

        ctx   = _ctx(name)
        leads = _db.get_leadstream_top_leads(name, limit=3)

        subject, body_text = _sassy_morning_copy(
            ctx=ctx, rank=rank, team_size=team_size,
            calls=agent["calls"], appts=agent["appts"], texts=agent["texts"],
            team_avg_calls=team_avg_calls, top_agent=top_agent, day_name=day_name,
        )
        html = _build_morning_html(body_text, leads, ctx.get("dashboard_url", ""))

        try:
            if not dry_run:
                api_key = os.environ.get("SENDGRID_API_KEY")
                from sendgrid import SendGridAPIClient
                from sendgrid.helpers.mail import Mail, Email as _Email
                msg = Mail(
                    from_email=EMAIL_FROM, to_emails=email,
                    subject=subject, plain_text_content=body_text, html_content=html,
                )
                # CC Barry so he can watch the nudges go out
                msg.personalizations[0].add_cc(_Email(EMAIL_FROM))
                SendGridAPIClient(api_key).send(msg)
            _db.log_nudge(name, "morning", subject, status="sent" if not dry_run else "dry_run")
            logger.info("Morning nudge [#%d/%d] → %s | %s", rank, team_size, name, subject[:60])
            sent += 1
        except Exception as e:
            _db.log_nudge(name, "morning", subject, status="failed")
            logger.warning("Morning nudge failed for %s: %s", name, e)

    logger.info("run_morning_nudges: sent %d emails", sent)
    return sent


def _sassy_morning_copy(ctx, rank, team_size, calls, appts, texts,
                         team_avg_calls, top_agent, day_name):
    """Return (subject, plain_text_body) based on yesterday's team rank.

    Each email randomly picks ONE tone and stays in it the whole way through:
      - 'funny'  : sassy, leaderboard trash talk, light teasing — no serious why
      - 'serious': leads with their goals/why/identity, no jokes
    """
    first        = ctx["first"]
    who          = ctx["who"]
    what_happens = ctx.get("what_happens") or ""
    why          = ctx.get("why") or ""
    identity     = ctx.get("identity") or "a top producer"
    gci_fmt      = ctx["gci_fmt"]
    top_first    = top_agent["name"].split()[0]
    top_calls    = top_agent["calls"]
    top_appts    = top_agent["appts"]
    score        = calls + texts + (appts * 3)
    is_zero      = score == 0
    is_last      = rank == team_size
    is_first     = rank == 1

    # Random tone for today — pick a lane and stay in it
    tone = random.choice(["funny", "serious"])

    # Funny-but-grounded kicker — ends every funny email with their why (clean, warm, punchy)
    if who and what_happens:
        who_kicker = random.choice([
            f"P.S. {what_happens[:60]}{'…' if len(what_happens) > 60 else ''} — that's the whole point. Go make some calls.",
            f"Now go do it for {who}. They're not going to care about your rank. They'll care about the result.",
            f"(Seriously though — {who} is counting on you. Make today count.)",
            f"Your {who} didn't sign up for a half-effort year. Neither did you. Let's go.",
            f"James Clear would say every call you make is a vote for the agent you're becoming. Cast some votes today. Your {who} is watching.",
        ])
    elif who:
        who_kicker = random.choice([
            f"(Seriously though — {who} is counting on you. Make today count.)",
            f"Now go do it for {who}. Let's go.",
            f"Your {who} didn't sign up for a half-effort year. Neither did you.",
        ])
    else:
        who_kicker = f"Now go make {gci_fmt} happen one conversation at a time."

    # Serious why-opener (only used in serious tone)
    if why and who and what_happens:
        why_hook = random.choice([
            f"You told us you're building toward this: \"{what_happens[:80]}{'…' if len(what_happens) > 80 else ''}\" — for {who}. That's the whole point.",
            f"You wrote: \"{why[:70]}{'…' if len(why) > 70 else ''}\"\n\nThat's why you're here. That's what every conversation is building toward.",
            f"You're chasing {gci_fmt} this year — not for a number on a spreadsheet, but because of what it unlocks for {who}.",
        ])
    elif who and gci_fmt:
        why_hook = f"You're after {gci_fmt} this year. That number means something real for {who}. It doesn't happen without the work."
    else:
        why_hook = f"You set a goal of {gci_fmt} this year. Every conversation is a brick. Every day counts."

    # ── ZERO activity ──────────────────────────────────────────────────
    if is_zero:
        if tone == "funny":
            subject = random.choice([
                f"Did you binge watch Netflix {day_name}? 📺",
                f"I checked FUB for you {day_name}... and checked again 👀",
                f"FUB says you took {day_name} off. Bold choice, {first}. 😅",
                f"Zero calls. Zero appointments. Just vibes? 🫠",
                f"Your leads called. FUB didn't see you pick up. 🤙",
                f"Alexa, play 'Where Did {first} Go' 🎵",
            ])
            body = random.choice([
                f"Haha kidding about the Netflix — but actually I'm not.\n\nFUB is showing zero conversations and zero appointments for you {day_name}. Zero. Zilch. Nada. The rest of the team was out there stacking calls while you were apparently in a parallel universe where real estate works differently.\n\nSpoiler: it doesn't.\n\nToday is a fresh start. Let's see you on the leaderboard.\n\n{who_kicker}",
                f"Look, I'm not saying you watched six hours of TV {day_name}. I'm just saying FUB has absolutely nothing to show for it, and the TV guide might.\n\n{top_first} led the team with {top_calls} calls. You had zero. James Clear would call this 'failing to cast a single vote for the agent you want to become.' I'd call it a tomorrow problem — except tomorrow is today.\n\n{who_kicker}",
                f"I ran the numbers for {day_name}. Double-checked. Triple-checked. Asked a colleague. Checked one more time.\n\nStill zero.\n\nLook, every great agent has an off day. The difference is they don't let it become an off week. Today is the bounce-back. The redemption arc. The sequel where {first} shows up.\n\n{who_kicker}",
            ])
        else:  # serious
            subject = random.choice([
                f"{first}, FUB showed nothing for you {day_name}. Let's fix that today.",
                f"No conversations logged {day_name}, {first}. Today matters.",
                f"{gci_fmt} doesn't happen on zero-call days, {first}.",
            ])
            body = random.choice([
                f"{why_hook}\n\nFUB is showing zero conversations and zero appointments for you {day_name}. That's not a step toward any of it.\n\nToday is. {who.capitalize() if who else 'The people counting on you'} don't get a pause button — and neither does {gci_fmt}. Get after it, {first}.",
                f"{why_hook}\n\nI ran the numbers for {day_name}. Still zero.\n\nEvery agent who's hit a big year has had off days. What separates them is what they do the very next morning. That's today. Make {day_name} irrelevant.",
            ])

    # ── LAST PLACE but tried ───────────────────────────────────────────
    elif is_last:
        if tone == "funny":
            subject = random.choice([
                f"Dead last on the team {day_name}. But at least you showed up 👏",
                f"While everyone called more than you... you did try a few 😅",
                f"#{rank} of {team_size} {day_name}. Technically still on the leaderboard.",
                f"Last place ribbon. But it's still a ribbon, {first}. 🎀",
            ])
            body = random.choice([
                f"I'll be honest — everyone else on the team outworked you {day_name}. But you logged {calls} conversation{'s' if calls != 1 else ''}{(' and ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}. So I respect the effort. Kind of. The scoreboard less so.\n\n{top_first} led with {top_calls} calls. The gap between last place and first place is almost always just reps — and today you've got a full day of them.\n\n{who_kicker}",
                f"Last place {day_name}. But here's the thing about last place — it's the best starting point for a comeback story. You know who loves a comeback story? Atomic Habits. You know who else? {who}.\n\n{calls} conversation{'s' if calls != 1 else ''} logged. Today you're going for {team_avg_calls + 5}. That's the whole plan. Simple.\n\n{who_kicker}",
            ])
        else:  # serious
            subject = random.choice([
                f"#{rank} of {team_size} {day_name}, {first}. You're built for better than that.",
                f"The team outworked you {day_name}. Time to flip the script, {first}.",
                f"{identity} doesn't finish last. Let's reset today, {first}.",
            ])
            body = random.choice([
                f"{why_hook}\n\nEveryone else on the team outworked you {day_name} — you landed at #{rank} of {team_size}. But {calls} conversation{'s' if calls != 1 else ''} is a start, not a ceiling.\n\n{top_first} led with {top_calls} calls. The gap isn't that wide. Close it today — {who} is worth more than last place.",
                f"{why_hook}\n\nBottom of the leaderboard {day_name}. That's the data — it's not the story.\n\n{identity} bounces back. Today is day one of that. Make {team_avg_calls + 5} conversations happen and let's see a different number tomorrow.",
            ])

    # ── BOTTOM HALF ────────────────────────────────────────────────────
    elif rank > team_size // 2:
        if tone == "funny":
            subject = random.choice([
                f"#{rank} of {team_size} {day_name}. The top is right there, {first} 📈",
                f"Solidly average {day_name}. Good is the enemy of great, {first}.",
                f"You blended in with the pack {day_name}. Stand out today 👊",
                f"#{rank} of {team_size}. Respectably mediocre. Let's fix that 😤",
            ])
            body = (
                f"#{rank} out of {team_size} {day_name} — {calls} conversation{'s' if calls != 1 else ''}"
                f"{(', ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}. "
                f"Solidly... middle. Like a ham sandwich. Totally fine. Not what you came here for.\n\n"
                f"Team average: {team_avg_calls}. {top_first} led with {top_calls}. "
                f"The difference between #{rank} and the top half is usually just one more focused hour — which you absolutely have today.\n\n"
                f"{who_kicker}"
            )
        else:  # serious
            subject = random.choice([
                f"#{rank} of {team_size} {day_name}. You're capable of more, {first}.",
                f"Middle of the pack {day_name}. {identity} belongs at the top.",
                f"The top half is one conversation away, {first}. Go get it.",
            ])
            body = (
                f"{why_hook}\n\n"
                f"#{rank} out of {team_size} {day_name} — {calls} conversation{'s' if calls != 1 else ''}"
                f"{(', ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}. "
                f"Team average: {team_avg_calls}. {top_first} led with {top_calls}.\n\n"
                f"{identity} doesn't finish in the bottom half. The difference between #{rank} and the top is one more focused hour. "
                f"You've got that in you today — and {who} is the reason to use it."
            )

    # ── TOP 3 (not #1) ─────────────────────────────────────────────────
    elif not is_first:
        if tone == "funny":
            subject = random.choice([
                f"Top {rank} {day_name}, {first} 🔥 {top_first}'s spot is right there",
                f"#{rank} on the team {day_name}. {top_first} is looking over their shoulder 👀",
                f"You were cooking {day_name}, {first}. Don't let up 🔥",
                f"Almost #1, {first}. {top_first} would like to have a word. 😤",
            ])
            body = (
                f"Top {rank} on the team {day_name} — {calls} conversation{'s' if calls != 1 else ''}"
                f"{(', ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}.\n\n"
                f"{top_first} edged you out with {top_calls} calls. ONE more conversation today and that flips. "
                f"You're in the zone. {top_first} is not sleeping great right now. Keep that energy.\n\n"
                f"{who_kicker}"
            )
        else:  # serious
            subject = random.choice([
                f"Top {rank} {day_name}, {first}. {top_first}'s spot is yours if you want it.",
                f"#{rank} on the team and building, {first}. Keep going.",
                f"You showed up {day_name}, {first}. {gci_fmt} is getting closer.",
            ])
            body = (
                f"{why_hook}\n\n"
                f"And {day_name}? You showed up — top {rank} on the team. "
                f"{calls} conversation{'s' if calls != 1 else ''}"
                f"{(', ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}.\n\n"
                f"{top_first} edged you out with {top_calls} calls. One more conversation today and {gci_fmt} gets closer — and so does everything {who} is counting on. "
                f"You're in the zone, {first}. Stay there."
            )

    # ── #1 ────────────────────────────────────────────────────────────
    else:
        if tone == "funny":
            subject = random.choice([
                f"👑 You ran laps around the team {day_name}, {first}",
                f"#1 on the team {day_name}. Bow down. 🏆",
                f"Team leaderboard {day_name}: {first} first. Everyone else: trying. 😤",
                f"Barry might actually high-five you for {day_name}, {first} 🙌",
                f"Did you even break a sweat yesterday, {first}? Because the scoreboard says no. 👀",
            ])
            body = random.choice([
                f"#1 out of {team_size}. You led the entire team {day_name} with {calls} conversation{'s' if calls != 1 else ''}{(' and ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}.\n\nEveryone else is in their feelings about the leaderboard right now. You should be in a good mood. Stack another one today and make it a habit.\n\n{who_kicker}",
                f"Top of the leaderboard {day_name}. {calls} calls. {appts} appointments. The rest of the team saw that and said 'oh, so we're doing THIS now.'\n\nYes. Yes you are. Don't stop.\n\n{who_kicker}",
                f"#1, {first}. Which means today every single person on this team is your competition. They saw the scoreboard. They're motivated.\n\nGood. So are you. Let's go.\n\n{who_kicker}",
            ])
        else:  # serious
            subject = random.choice([
                f"#1 on the team {day_name}, {first}. That's {identity} showing up.",
                f"You led the team {day_name}, {first}. This is what {gci_fmt} looks like.",
                f"{first}, you showed {who} something real {day_name}. Do it again.",
            ])
            body = random.choice([
                f"{why_hook}\n\n{day_name} you proved it — #1 out of {team_size}. {calls} conversation{'s' if calls != 1 else ''}{(' and ' + str(appts) + ' appointment' + ('s' if appts != 1 else '')) if appts > 0 else ''}. That's {identity} in action.\n\nEveryone else is gunning for your spot today. Defend it, {first}. {who.capitalize() if who else 'The people counting on you'} deserve that version of you every single day.",
                f"{why_hook}\n\nTop of the leaderboard {day_name}. {calls} calls. {appts} appointments. That's what {gci_fmt} looks like when it's being built right.\n\nThe best agents don't coast after a big day — they stack another one on top of it. Do it again today, {first}.",
            ])

    return subject, body


def _build_morning_html(body_text: str, leads: list, dashboard_url: str) -> str:
    """Build the full branded HTML email for the morning nudge."""
    FUB_PERSON_URL = "https://yourfriendlyagent.followupboss.com/2/people/view/{person_id}"
    FUB_LIST_URL   = "https://yourfriendlyagent.followupboss.com/2/people?smart-list=leadstream"

    html_body = "<p>" + body_text.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"

    # LeadStream leads section
    leads_html = ""
    if leads:
        lead_rows = ""
        for lead in leads:
            pid   = lead.get("id")
            lname = lead.get("name", "Unknown")
            score = lead.get("score", 0)
            tier  = (lead.get("tier") or "").lower()
            stage = lead.get("stage", "")
            url   = FUB_PERSON_URL.format(person_id=pid) if pid else "#"
            tier_color = {"hot": "#ef4444", "warm": "#f97316", "active": "#3b82f6"}.get(tier, "#6b7280")
            lead_rows += f"""
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid #f1f5f9">
                <a href="{url}" style="font-size:15px;font-weight:600;color:#1a1a2e;text-decoration:none">{lname}</a>
                <span style="margin-left:8px;font-size:11px;font-weight:700;color:{tier_color};text-transform:uppercase;background:rgba(0,0,0,0.05);padding:2px 7px;border-radius:10px">{tier}</span><br>
                <span style="font-size:12px;color:#94a3b8">{stage} &nbsp;·&nbsp; Score: {score}</span>
              </td>
              <td style="padding:10px 0;border-bottom:1px solid #f1f5f9;text-align:right;vertical-align:middle">
                <a href="{url}" style="font-size:12px;font-weight:700;color:#667eea;text-decoration:none">Open in FUB →</a>
              </td>
            </tr>"""

        leads_html = f"""
  <div style="background:#f8fafc;border-radius:10px;padding:20px 24px;margin:24px 0">
    <p style="margin:0 0 14px;font-size:12px;font-weight:700;color:#94a3b8;letter-spacing:1px;text-transform:uppercase">🔥 Your Top LeadStream Leads</p>
    <table width="100%" cellpadding="0" cellspacing="0">{lead_rows}
    </table>
    <p style="margin:16px 0 0;text-align:center">
      <a href="{FUB_LIST_URL}" style="font-size:13px;font-weight:700;color:#667eea;text-decoration:none">See all your LeadStream leads →</a>
    </p>
  </div>"""

    dash_btn = f"""
  <div style="text-align:center;margin:20px 0 8px">
    <a href="{dashboard_url}" style="display:inline-block;background:#f5a623;color:#0d1117;padding:13px 32px;border-radius:8px;text-decoration:none;font-weight:800;font-size:14px">View My Dashboard →</a>
  </div>""" if dashboard_url else ""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<div style="max-width:560px;margin:24px auto;background:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
  <div style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:22px 32px;text-align:center">
    <img src="{LOGO_URL}" alt="Legacy Home Team" width="120" style="display:block;margin:0 auto;height:auto">
  </div>
  <div style="padding:28px 32px 12px">
    <div style="font-size:16px;line-height:1.75;color:#2d3748">{html_body}</div>
    {leads_html}
    {dash_btn}
  </div>
  <div style="background:#f7fafc;padding:14px 32px;text-align:center;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:12px;color:#a0aec0">Legacy Home Team &middot; Daily accountability &middot; <a href="{dashboard_url}" style="color:#a0aec0">My Dashboard</a></p>
  </div>
</div>
</body></html>"""


def run_missed_day_check(dry_run: bool = False):
    """
    Called at 5pm ET. For any agent who hasn't logged activity today,
    send a gentle missed-day nudge.
    """
    profiles = _db.get_agent_profiles(active_only=True)
    sent = 0
    for p in profiles:
        name  = p["agent_name"]
        email = p.get("email")
        if not email:
            continue
        act = _db.get_todays_activity(name)
        if act["calls"] == 0 and act["texts"] == 0 and act["appts"] == 0:
            if nudge_agent(name, "missed_day", email, dry_run=dry_run):
                sent += 1
    logger.info("run_missed_day_check: sent %d nudge emails", sent)
    return sent


def run_streak_break_check(dry_run: bool = False):
    """
    Called each morning. If an agent's last_activity_date was 2+ days ago,
    their streak broke — send the streak-break nudge.
    """
    profiles = _db.get_agent_profiles(active_only=True)
    today    = date.today()
    sent = 0
    for p in profiles:
        name  = p["agent_name"]
        email = p.get("email")
        if not email:
            continue
        streak = _db.get_streak(name)
        last   = streak.get("last_activity_date")
        if not last:
            continue
        days_since = (today - date.fromisoformat(last)).days
        if days_since == 1 and streak.get("current_streak", 0) == 0:
            # Broke it yesterday — nudge this morning
            if nudge_agent(name, "streak_break", email, dry_run=dry_run):
                sent += 1
    logger.info("run_streak_break_check: sent %d nudge emails", sent)
    return sent


def run_weekly_summary(dry_run: bool = False):
    """
    Called Sunday 6pm ET. Send each agent their week-in-numbers summary.
    """
    profiles = _db.get_agent_profiles(active_only=True)
    sent = 0
    for p in profiles:
        name  = p["agent_name"]
        email = p.get("email")
        if not email:
            continue

        activity = _db.get_daily_activity(name, days=7)
        calls = sum(r["calls"] for r in activity)
        appts = sum(r["appts"] for r in activity)

        # Pace context
        goal    = _db.get_goal(name, year=date.today().year)
        targets = _db.compute_targets(goal) if goal else {}
        ytd     = _db.get_ytd_cache(year=date.today().year).get(name, {})
        actuals = {
            "calls_ytd": ytd.get("calls_ytd", 0),
            "appts_ytd": ytd.get("appts_ytd", 0),
            "closings_ytd": 0,
        }
        pace = _db.compute_pace(goal, targets, actuals) if goal else {}
        overall_pct = pace.get("overall_pct", 0)
        pace_word = "ahead of" if overall_pct >= 100 else ("on" if overall_pct >= 85 else "behind")
        why = _db.get_agent_why(name) or {}
        one_liner = ""
        if why.get("why_statement"):
            if overall_pct >= 90:
                one_liner = f"You're building toward: \"{why['why_statement'][:50]}…\""
            else:
                one_liner = f"Next week: get back to your why."

        extra = {"calls": calls, "appts": appts, "pace_word": pace_word, "one_liner": one_liner}
        if nudge_agent(name, "weekly_summary", email, extra=extra, dry_run=dry_run):
            sent += 1
    logger.info("run_weekly_summary: sent %d nudge emails", sent)
    return sent


def run_plateau_check(dry_run: bool = False):
    """
    Weekly check: if 2+ weeks of activity within 10% variance (no growth), send plateau nudge.
    """
    profiles = _db.get_agent_profiles(active_only=True)
    sent = 0
    for p in profiles:
        name  = p["agent_name"]
        email = p.get("email")
        if not email:
            continue
        activity = _db.get_daily_activity(name, days=21)
        if len(activity) < 10:
            continue
        weeks = [
            [r["calls"] for r in activity if _days_ago(r["date"]) <= 7],
            [r["calls"] for r in activity if 7 < _days_ago(r["date"]) <= 14],
            [r["calls"] for r in activity if 14 < _days_ago(r["date"]) <= 21],
        ]
        totals = [sum(w) for w in weeks if w]
        if len(totals) < 2:
            continue
        max_t = max(totals) or 1
        min_t = min(totals)
        variance = (max_t - min_t) / max_t
        if variance <= 0.10 and totals[0] <= totals[-1]:  # flat or declining
            ctx = _ctx(name)
            msg = (
                f"Your effort has been steady, {ctx['first']}. "
                "Results sometimes lag behind the work. Atomic Habits calls this the valley of disappointment. "
                "The compound effect is building — don't stop now."
            )
            _db.log_nudge(name, "plateau", msg)
            if not dry_run:
                try:
                    subject = _subject("plateau", ctx)
                    _send_email(email, subject, msg)
                    sent += 1
                except Exception as e:
                    logger.warning("plateau nudge email failed for %s: %s", name, e)
    return sent


def run_post_closing_followups(dry_run: bool = False):
    """Check for due 30/60/90-day post-closing follow-up reminders."""
    due = _db.get_due_followups(as_of=date.today())
    profiles = {p["agent_name"]: p for p in _db.get_agent_profiles(active_only=True)}
    sent = 0
    for item in due:
        name   = item["agent_name"]
        email  = (profiles.get(name) or {}).get("email")
        client = item["client_name"] or "your recent client"
        for days in item["due_days"]:
            ctx = _ctx(name)
            msg = MILESTONE_TEMPLATES["post_closing"].format(client=client, **ctx)
            if email:
                try:
                    if not dry_run:
                        subject = _subject("post_closing", ctx)
                        _send_email(email, subject, msg)
                        _db.log_nudge(name, f"post_closing_{days}d", msg, status="sent")
                    _db.mark_followup_sent(item["id"], days)
                    sent += 1
                except Exception as e:
                    logger.warning("post_closing nudge email failed for %s: %s", name, e)
    return sent


# ---------------------------------------------------------------------------
# Milestone triggers (called from app.py when events occur)
# ---------------------------------------------------------------------------

def trigger_milestone(agent_name: str, milestone_key: str,
                       email: str, extra: dict = None, dry_run: bool = False):
    """
    milestone_key: 'first_close' | 'streak_7' | 'streak_14' | 'streak_21' |
                   'streak_30' | 'pace_ahead' | 'post_closing'
    """
    ctx = _ctx(agent_name)
    if extra:
        ctx.update(extra)
    tmpl = MILESTONE_TEMPLATES.get(milestone_key, "Great work, {first}! Keep going.")
    try:
        msg = tmpl.format(**ctx)
    except Exception:
        msg = f"Great milestone, {ctx['first']}! Keep going."
    nudge_type = f"milestone_{milestone_key}"
    subject = _subject(nudge_type, ctx)
    if email:
        try:
            result = _send_email(email, subject, msg, dry_run=dry_run)
            _db.log_nudge(agent_name, nudge_type, msg, status=result.get("status", "sent"))
        except Exception as e:
            logger.warning("trigger_milestone email failed for %s: %s", agent_name, e)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _et_now():
    """Current time in US/Eastern."""
    m = datetime.now(timezone.utc).month
    offset = -4 if 3 <= m <= 11 else -5
    return datetime.now(timezone(timedelta(hours=offset)))


def _days_ago(date_str: str) -> int:
    """How many days ago was date_str (YYYY-MM-DD)?"""
    return (date.today() - date.fromisoformat(date_str)).days
