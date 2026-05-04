"""
Legacy Home Team — Arc Engine (Phase 4)

7-arc motivation framework for morning nudge emails.
Each arc is a different lens: identity, purpose, scoreboard,
compound, comeback, elite standard, deal math.

Selection is weighted by the agent's current situation
(pace, streak, trend, rank, archetype) with freshness
rules to prevent repetition.

Voice: Barry Jenkins — Too Nice for Sales
  Conversational. Story-grounded. Teaching > pushing.
  Warm but won't let you off the hook.
  Short punchy sentences + rhetorical questions.
  "You're being too nice to your comfort zone."
"""
import random
import logging
from datetime import date

logger = logging.getLogger(__name__)

ARCS = ["identity", "purpose", "scoreboard", "compound", "comeback", "elite", "deal_math"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_gci(n):
    """Format a dollar amount as $XK or $X.XM."""
    if not n:
        return "your goal"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${round(n / 1_000)}K"
    return f"${round(n)}"


def _q_season():
    """Return current quarter label: q1, q2, q3, q4."""
    m = date.today().month
    if m <= 3:
        return "q1"
    if m <= 6:
        return "q2"
    if m <= 9:
        return "q3"
    return "q4"


# ---------------------------------------------------------------------------
# Situation detection
# ---------------------------------------------------------------------------

def detect_situation(agent_ctx, goal_ctx, deal_summary, streak_data,
                     rank, team_size, calls, trend_data):
    """
    Analyse the agent's current state and return a situation dict that
    the arc selection algorithm uses to pick the right motivation lens.

    Parameters
    ----------
    agent_ctx   : dict from nudge_engine._ctx()
    goal_ctx    : dict from nudge_engine._build_goal_ctx() or None
    deal_summary: dict from db.get_deal_summary()
    streak_data : dict from db.get_streak()
    rank        : int, 1-based position on yesterday's leaderboard
    team_size   : int, total agents on leaderboard
    calls       : int, calls made yesterday
    trend_data  : str, one of 'improving' / 'declining' / 'stagnant' / 'unknown'
    """
    streak       = int((streak_data or {}).get("current_streak", 0))
    last_date    = (streak_data or {}).get("last_activity_date")
    has_goals    = goal_ctx is not None
    has_why      = bool((agent_ctx or {}).get("why"))
    closings_ytd = int((deal_summary or {}).get("closings", 0))
    has_deals    = closings_ytd > 0
    pace_pct     = float((goal_ctx or {}).get("calls_pace_pct", 0)) if has_goals else 0
    archetype    = (agent_ctx or {}).get("identity", "")

    # Map identity label back to archetype key for arc weighting
    archetype_map = {
        "The Closer":             "closer",
        "The Prospecting Machine": "prospecting_machine",
        "The Consistent One":     "consistent",
        "The Comeback Story":     "comeback_story",
        "The Relationship Builder": "relationship_builder",
    }
    archetype_key = archetype_map.get(archetype, archetype.lower().replace(" ", "_"))

    # Pace state
    if has_goals and pace_pct < 60:
        pace_state = "way_behind"
    elif has_goals and pace_pct < 85:
        pace_state = "behind"
    elif has_goals and pace_pct >= 100:
        pace_state = "ahead"
    else:
        pace_state = "on_track"

    # Streak state
    zero_day = calls == 0
    if streak == 0 and last_date:
        streak_state = "just_broke"
    elif streak >= 5:
        streak_state = "on_streak"
    else:
        streak_state = "building"

    # Rank tier
    if rank == 1:
        rank_tier = "top1"
    elif rank <= max(2, round(team_size * 0.25)):
        rank_tier = "top25"
    elif rank >= team_size:
        rank_tier = "last"
    elif rank > team_size // 2:
        rank_tier = "bottom_half"
    else:
        rank_tier = "mid"

    return {
        "pace_state":     pace_state,
        "streak_state":   streak_state,
        "streak":         streak,
        "zero_day":       zero_day,
        "activity_trend": trend_data or "unknown",
        "rank_tier":      rank_tier,
        "rank":           rank,
        "team_size":      team_size,
        "has_goals":      has_goals,
        "has_why":        has_why,
        "has_deals":      has_deals,
        "closings_ytd":   closings_ytd,
        "archetype":      archetype_key,
        "q_season":       _q_season(),
    }


# ---------------------------------------------------------------------------
# Arc selection
# ---------------------------------------------------------------------------

def select_arc(situation, recent_arcs):
    """
    Weighted random arc selection based on agent situation.
    Never repeats yesterday's arc. Diminishing weights for arcs used
    in the last 3 days.

    Returns arc name string: one of ARCS.
    """
    weights = {
        "identity":   30,
        "purpose":    25,
        "scoreboard": 25,
        "compound":   15,
        "comeback":   10,
        "elite":      20,
        "deal_math":  15,
    }

    # Situation boosters
    if situation["pace_state"] == "way_behind":
        weights["scoreboard"] += 35
        weights["comeback"]   += 25
    elif situation["pace_state"] == "behind":
        weights["scoreboard"] += 15
        weights["deal_math"]  += 10

    if situation["streak_state"] == "just_broke":
        weights["comeback"] += 45
        weights["purpose"]  += 25
    elif situation["streak_state"] == "on_streak":
        weights["identity"] += 25
        weights["compound"] += 20

    if situation["activity_trend"] == "declining":
        weights["comeback"] += 30
        weights["purpose"]  += 20
    elif situation["activity_trend"] == "improving":
        weights["compound"]  += 25
        weights["identity"]  += 15

    if situation["rank_tier"] == "last":
        weights["elite"]    += 30
        weights["comeback"] += 20
    elif situation["rank_tier"] == "top1":
        weights["identity"] += 30
        weights["compound"] += 20

    if situation["zero_day"]:
        weights["purpose"] += 35  # hit them with the why on a zero day

    archetype = situation.get("archetype", "")
    if archetype in ("closer", "prospecting_machine"):
        weights["elite"]      += 25
        weights["scoreboard"] += 20
    elif archetype == "comeback_story":
        weights["comeback"] += 30
    elif archetype == "consistent":
        weights["identity"] += 25
        weights["compound"] += 20

    if situation.get("has_deals") and situation.get("closings_ytd", 0) > 0:
        weights["deal_math"] += 20

    q = situation.get("q_season", "")
    if q == "q3":
        weights["comeback"]   += 15
        weights["scoreboard"] += 10
    elif q == "q4":
        weights["deal_math"]  += 25
        weights["scoreboard"] += 25

    # Freshness: never same arc 2 days in a row, diminishing returns after that
    if recent_arcs:
        if recent_arcs[0] in weights:
            weights[recent_arcs[0]] = 0  # hard block: yesterday
        if len(recent_arcs) > 1 and recent_arcs[1] in weights:
            weights[recent_arcs[1]] = weights[recent_arcs[1]] // 2
        if len(recent_arcs) > 2 and recent_arcs[2] in weights:
            weights[recent_arcs[2]] = int(weights[recent_arcs[2]] * 0.75)

    # No goal = can't use scoreboard/deal_math/compound fully
    if not situation.get("has_goals"):
        weights["scoreboard"] = min(weights["scoreboard"], 20)
        weights["deal_math"]  = min(weights["deal_math"],  15)
        weights["compound"]   = min(weights["compound"],   15)

    if not situation.get("has_why"):
        weights["purpose"] = min(weights["purpose"], 15)

    # Weighted random selection
    total = sum(max(0, w) for w in weights.values())
    if total == 0:
        return "scoreboard"
    r = random.uniform(0, total)
    cumulative = 0
    for arc, w in weights.items():
        cumulative += max(0, w)
        if r <= cumulative:
            return arc
    return "scoreboard"


# ---------------------------------------------------------------------------
# Email assembly
# ---------------------------------------------------------------------------

def build_arc_email(arc, ctx, situation, goal_ctx, deal_summary, tone,
                    top_agent, team_avg, day_name):
    """
    Build a (subject, body) tuple for the given arc.

    arc        : one of ARCS
    ctx        : agent context dict from nudge_engine._ctx()
    situation  : output of detect_situation()
    goal_ctx   : output of _build_goal_ctx() or None
    deal_summary: output of db.get_deal_summary()
    tone       : 'funny' or 'serious'
    top_agent  : leaderboard dict with 'name', 'calls'
    team_avg   : int, team average calls yesterday
    day_name   : str, e.g. 'Monday'
    """
    arc_fn = {
        "identity":   _arc_identity,
        "purpose":    _arc_purpose,
        "scoreboard": _arc_scoreboard,
        "compound":   _arc_compound,
        "comeback":   _arc_comeback,
        "elite":      _arc_elite,
        "deal_math":  _arc_deal_math,
    }.get(arc, _arc_scoreboard)

    return arc_fn(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name)


def _goal_section(ctx, goal_ctx, tone):
    """
    Compact 1-2 sentence goal section prepended when has_goals=True.
    Returns empty string when goal_ctx is None.
    """
    if not goal_ctx:
        return ""

    daily_target     = goal_ctx["daily_target"]
    calls_pace_pct   = goal_ctx["calls_pace_pct"]
    calls_ytd        = goal_ctx["calls_ytd"]
    calls_target_ytd = goal_ctx["calls_target_ytd"]
    gci_goal_fmt     = goal_ctx["gci_fmt"]
    gap              = goal_ctx["gap_yesterday"]

    if calls_pace_pct >= 100:
        pace_label = "ahead of pace"
    elif calls_pace_pct >= 85:
        pace_label = "right on pace"
    elif calls_pace_pct >= 70:
        pace_label = "a little behind"
    else:
        pace_label = "behind pace"

    if gap > 0:
        gap_label = f"{gap} above your {daily_target}/day target"
    elif gap == 0:
        gap_label = f"right on your {daily_target}/day target"
    else:
        gap_label = f"{abs(gap)} short of your {daily_target}/day target"

    if tone == "funny":
        opts = [
            f"Quick scoreboard on {gci_goal_fmt}: need {daily_target} dials/day, you had {calls_ytd} YTD vs. a target of {calls_target_ytd}. Pace: {calls_pace_pct}% ({pace_label}). {'The math is mathing.' if calls_pace_pct >= 85 else 'The math needs help. Today is your audition.'}",
            f"Your {gci_goal_fmt} doesn't care about your feelings. It needs {daily_target} dials a day. YTD pace: {calls_pace_pct}% ({pace_label}). {'Green light, keep going.' if calls_pace_pct >= 85 else 'Yellow flag. Today is your pit stop.'}",
        ]
    else:
        opts = [
            f"Your {gci_goal_fmt} goal needs {daily_target} dials a day. YTD pace: {calls_pace_pct}% — {pace_label} ({calls_ytd} dials vs. a target of {calls_target_ytd} at this point in the year).",
            f"To reach {gci_goal_fmt} this year, the daily dial target is {daily_target}. You're sitting at {calls_pace_pct}% of pace YTD — {pace_label}. The gap closes one day at a time.",
        ]
    return random.choice(opts)


# ---------------------------------------------------------------------------
# Arc 1: IDENTITY
# ---------------------------------------------------------------------------

def _arc_identity(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'Every call is a vote for who you're becoming.'
    Best when: on a streak, improving trend, identity is set.
    """
    first    = ctx["first"]
    identity = ctx.get("identity") or "a top producer"
    streak   = ctx.get("streak", 0)
    who      = ctx.get("who", "the people you love")
    calls_t  = ctx.get("daily_calls", 20)
    has_goals = goal_ctx is not None
    has_why   = situation.get("has_why")

    goal_pre = _goal_section(ctx, goal_ctx, tone)
    goal_block = f"{goal_pre}\n\n" if has_goals else ""

    if tone == "funny":
        subjects = [
            f"{first}, {identity} doesn't take Tuesdays off. Just checking. 👀",
            f"The scoreboard doesn't vote on who {first} is. The calls do.",
            f"Every unanswered call is {identity} taking a sick day. Don't. 📞",
            f"Who are you becoming today, {first}? FUB is keeping score. 🏆",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"James Clear didn't write 400 pages to tell you to 'stay motivated.' "
                f"He said: every action you take is a vote for the person you're becoming.\n\n"
                f"You told us you're {identity}. That identity doesn't get to show up only when you feel like it. "
                f"It shows up on the Tuesdays that don't feel like it. It shows up when the leads don't call back. "
                f"It shows up right now.\n\n"
                f"You've made {streak} days of votes. Today is another ballot. Cast it.\n\n"
                f"(Your {who} would like to confirm the vote count is still going up.)"
            ),
            (
                f"{goal_block}"
                f"Here's the deal with identity — you don't rise to the level of your goals, you fall to the level of your systems.\n\n"
                f"You said your system is {calls_t} calls a day. {identity} has that system. "
                f"Today FUB is going to tell me whether {first} is currently {identity} or {identity}-adjacent.\n\n"
                f"Be the noun, not the aspiration. Go make {calls_t} calls."
            ),
            (
                f"{goal_block}"
                f"Quick question: if someone followed you around all day today and watched how you spent your time — "
                f"would they say 'oh yeah, that's {identity}'?\n\n"
                f"Because that's literally the test. Not the year-end number. Not the closing. Today's behavior.\n\n"
                f"Are you being too nice to your comfort zone right now? Make the call. Cast the vote."
            ),
        ]
        if has_why and who:
            who_kicker = random.choice([
                f"\n\nP.S. {who.capitalize()} didn't sign up for a halfway version of you. Neither did you.",
                f"\n\n(Your {who} are quietly rooting for the version of you that picks up the phone.)",
            ])
            bodies = [b + who_kicker for b in bodies]

    else:  # serious
        subjects = [
            f"Every call today is a vote for who {first} is becoming.",
            f"{first}, {identity} shows up on the hard days. Today is one.",
            f"You said you're {identity}. Prove it before 10am, {first}.",
            f"The agents who change their life do it one call at a time, {first}.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"You told us you're {identity}.\n\n"
                f"That identity doesn't get built on your best days — it gets confirmed on your average ones. "
                f"The days when the motivation isn't there. When the list feels stale. "
                f"When it's easier to find a reason to wait.\n\n"
                f"James Clear calls every action a vote. {streak} days of votes is a real streak. "
                f"Today is the day you defend what you've built — not for the number, but for the identity.\n\n"
                f"Make {calls_t} calls today, {first}. That's {identity} doing its job."
            ),
            (
                f"{goal_block}"
                f"The agents who outsell everyone else aren't smarter. They just stopped being too nice to their call list.\n\n"
                f"That's the whole lesson from 20 years in this business. The ones who win aren't the ones with the best leads "
                f"or the hottest zip codes. They're the ones who decided who they were going to be — and then showed up as that person every single morning.\n\n"
                f"You're {identity}. What does that agent do this morning?\n\nGo do that."
            ),
            (
                f"{goal_block}"
                f"There's a version of you that hits every goal this year and changes things for {who}.\n\n"
                f"That version isn't waiting for the right morning to show up. They already decided. "
                f"And every call they make today is proof of that decision.\n\n"
                f"Be that version, {first}. {calls_t} calls. Let's build."
            ),
        ]
        if has_why:
            why_close = f"\n\nRemember what you're building this for. Every conversation today is a brick in that wall."
            bodies = [b + why_close for b in bodies]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 2: PURPOSE
# ---------------------------------------------------------------------------

def _arc_purpose(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'This isn't about a number on a spreadsheet.'
    Best when: slumping, streak broke, zero day.
    """
    first        = ctx["first"]
    who          = ctx.get("who") or "the people you love"
    what_happens = ctx.get("what_happens") or "your goals become real"
    why          = ctx.get("why") or ""
    gci_fmt      = ctx.get("gci_fmt") or "your goal"
    calls_t      = ctx.get("daily_calls", 20)
    has_goals    = goal_ctx is not None

    goal_pre   = _goal_section(ctx, goal_ctx, tone)
    goal_block = f"{goal_pre}\n\n" if has_goals else ""

    if tone == "funny":
        subjects = [
            f"{first}, FUB can't see {who}. But that's exactly why you're here.",
            f"Your spreadsheet doesn't know about {who}. Your calls do.",
            f"The real reason you do this, {first}. (It's not GCI.) 💛",
            f"Somewhere in your pipeline is the deal that changes things for {who}.",
        ]
        subject = random.choice(subjects)

        what_short = what_happens[:80] + ("..." if len(what_happens) > 80 else "")
        bodies = [
            (
                f"{goal_block}"
                f"Let me tell you what FUB can't show you.\n\n"
                f"It can't show you {who} when \"{what_short}\" becomes real. "
                f"It can't show you what that moment feels like.\n\n"
                f"But somewhere in your database right now — buried in the 'nurture' pile, half-forgotten — "
                f"is a conversation that leads there. It's just a phone call you haven't made yet.\n\n"
                f"Make {calls_t} of them today. One of them might be the one.\n\n"
                f"(Too nice to make the call? I'm asking nicely. For {who}.)"
            ),
            (
                f"{goal_block}"
                f"Real talk, {first}: the math of real estate is simple. The doing is not.\n\n"
                f"You know the numbers. You know what {calls_t} calls a day compounds into. "
                f"You know what closing this year means for {who}.\n\n"
                f"The gap between knowing and doing? That's the whole game. "
                f"And the agents who close it aren't more motivated than you — they just decided the why was bigger than the discomfort.\n\n"
                f"Is \"{what_short}\" bigger than making the calls today?\n\nI think it is."
            ),
            (
                f"{goal_block}"
                f"Fun fact: I've never met an agent who regretted making their calls.\n\n"
                f"I have met a lot of agents who regretted not making them. Who gave away their best lead-gen hours to scrolling "
                f"and 'getting ready to get ready.'\n\n"
                f"Your {who} don't get a placeholder. They need the real thing. "
                f"Today's calls are the real thing. Go.\n\n"
                f"({calls_t} dials. Clock is running.)"
            ),
        ]

    else:  # serious
        subjects = [
            f"This isn't about GCI, {first}. It's about {who}.",
            f"{first}, the reason to pick up the phone today has nothing to do with a number.",
            f"What you're really building toward, {first}.",
            f"The deal that changes things for {who} is a conversation away, {first}.",
        ]
        subject = random.choice(subjects)

        what_short = what_happens[:80] + ("..." if len(what_happens) > 80 else "")
        why_short  = why[:80] + ("..." if len(why) > 80 else "")

        bodies = [
            (
                f"{goal_block}"
                f"Somewhere in your FUB database right now is the commission that changes things for {who}.\n\n"
                f"It's just a conversation you haven't had yet.\n\n"
                f"I'm not going to push you into picking up the phone. I learned a long time ago that pushing doesn't work. "
                f"What works is remembering why you're here in the first place.\n\n"
                f"You said: \"{what_short}\"\n\n"
                f"That's the reason. Make {calls_t} calls today, {first}. Not for GCI — for that."
            ),
            (
                f"{goal_block}"
                f"When I was {(why_short if why_short else 'building this team')}, I had to learn something the hard way: "
                f"the numbers don't motivate anyone for long.\n\n"
                f"What motivates people is the story behind the number. The face behind the goal.\n\n"
                f"Your story is \"{what_short}\" — and it doesn't happen without the calls. "
                f"You're not making {calls_t} dials today to hit a metric. "
                f"You're doing it because that future is worth the discomfort of one hour on the phone.\n\n"
                f"Pick up, {first}."
            ),
            (
                f"{goal_block}"
                f"The agents who outsell their coworkers — I've watched them for two decades — they all have one thing in common.\n\n"
                f"They're grateful. Genuinely. They know what they're building and who it's for. "
                f"That gratitude turns into calls. The calls turn into closings. "
                f"The closings turn into exactly what {who} needs.\n\n"
                f"You told us what you're building toward: \"{what_short}\"\n\n"
                f"That's {calls_t} calls. Today. Right now. Let's go."
            ),
        ]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 3: SCOREBOARD
# ---------------------------------------------------------------------------

def _arc_scoreboard(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'Here's exactly where you stand. Here's exactly what fixes it.'
    Best when: behind pace, data-oriented, anytime.
    """
    first       = ctx["first"]
    who         = ctx.get("who") or "the people you love"
    gci_fmt     = ctx.get("gci_fmt") or "your goal"
    calls_t     = ctx.get("daily_calls", 20)
    has_goals   = goal_ctx is not None
    rank        = situation.get("rank", 1)
    team_size   = situation.get("team_size", 1)
    top_first   = top_agent["name"].split()[0] if top_agent else "the top agent"
    top_calls   = top_agent["calls"] if top_agent else team_avg

    if has_goals:
        daily_target     = goal_ctx["daily_target"]
        calls_pace_pct   = goal_ctx["calls_pace_pct"]
        calls_ytd        = goal_ctx["calls_ytd"]
        calls_target_ytd = goal_ctx["calls_target_ytd"]
        gci_goal_fmt     = goal_ctx["gci_fmt"]
        gap              = goal_ctx["gap_yesterday"]
        behind_calls     = max(0, calls_target_ytd - calls_ytd)

        if calls_pace_pct >= 100:
            pace_line = f"YTD pace: {calls_pace_pct}% — ahead of schedule."
        elif calls_pace_pct >= 85:
            pace_line = f"YTD pace: {calls_pace_pct}% — right on track."
        else:
            days_left = max(1, (date(date.today().year, 12, 31) - date.today()).days)
            extra_per_day = round(behind_calls / days_left) if days_left > 0 else 0
            pace_line = (
                f"YTD pace: {calls_pace_pct}% — behind by {behind_calls} calls. "
                f"Adding {extra_per_day} extra calls/day closes the gap."
            )
    else:
        daily_target   = calls_t
        gci_goal_fmt   = gci_fmt
        calls_pace_pct = 0
        pace_line      = ""
        gap            = 0

    if tone == "funny":
        subjects = [
            f"The scoreboard is right there, {first}. Here's what it says. 📊",
            f"No fluff. Just math. And the math is talking, {first}.",
            f"The numbers don't lie. (Sometimes they're just a little brutal.) 📉",
            f"{first}'s {gci_goal_fmt} check-in. Spoiler: you can still do this.",
        ]
        subject = random.choice(subjects)

        if has_goals:
            bodies = [
                (
                    f"No pep talk today. Just the numbers.\n\n"
                    f"Goal: {gci_goal_fmt}. Daily dial target: {daily_target}. Yesterday: {gap + daily_target} "
                    f"({'above' if gap >= 0 else 'below'} target). {pace_line}\n\n"
                    f"The math is simple. The doing is not. But you already know the math — "
                    f"so the only variable left is whether you pick up the phone today.\n\n"
                    f"(The scoreboard is waiting, {first}. It's not judging you. It's just counting.)"
                ),
                (
                    f"I looked at your numbers. Then I looked again. Here's what's real:\n\n"
                    f"{pace_line}\n\n"
                    f"Daily dial target to hit {gci_goal_fmt}: {daily_target}. "
                    f"The gap between where you are and where you need to be is just reps. "
                    f"No mystery. No magic. Just reps.\n\n"
                    f"Make {daily_target} dials today. Log them. Repeat. That's the entire plan.\n\n"
                    f"(Told you. No fluff.)"
                ),
                (
                    f"Quick math lesson:\n\n"
                    f"{gci_goal_fmt} goal ÷ your avg commission = closings needed.\n"
                    f"Closings ÷ your conversion rate = appointments needed.\n"
                    f"Appointments ÷ your show rate = calls needed.\n"
                    f"Calls needed ÷ working days = {daily_target} dials/day.\n\n"
                    f"{pace_line}\n\n"
                    f"See? Simple. The doing is your job. {daily_target} dials. Go."
                ),
            ]
        else:
            bodies = [
                (
                    f"#{rank} of {team_size} on the team yesterday. Team average: {team_avg} calls. "
                    f"{top_first} led with {top_calls}.\n\n"
                    f"The math is simple: the gap between where you are and where you want to be is just reps. "
                    f"Today you get a new set of reps. Use them.\n\n"
                    f"(The scoreboard doesn't care about your mood. Neither does your potential.)"
                ),
            ]

    else:  # serious
        subjects = [
            f"Here's exactly where you stand, {first}. And here's what fixes it.",
            f"The math is clear, {first}. Here's what it says and what comes next.",
            f"No spin, {first}. Just the numbers and the path forward.",
            f"Transparent scorecard for {first} — and a specific action that moves it.",
        ]
        subject = random.choice(subjects)

        if has_goals:
            bodies = [
                (
                    f"Here's where you are right now on {gci_goal_fmt}:\n\n"
                    f"Daily dial target: {daily_target}. {pace_line}\n\n"
                    f"The gap is real. I'm not going to pretend it isn't. But a gap is just a math problem — "
                    f"and math problems have solutions.\n\n"
                    f"Today's solution: {daily_target} dials. Logged. That's one day's brick laid.\n\n"
                    f"The agents who hit their goals don't close the gap all at once. "
                    f"They close it one morning at a time. This is one of those mornings."
                ),
                (
                    f"I want to be straight with you, {first}.\n\n"
                    f"{pace_line}\n\n"
                    f"If nothing changes, the year ends differently than you planned. "
                    f"But this is the exact moment where things can change — not at year-end, not next month. Right now.\n\n"
                    f"The daily dial target to hit {gci_goal_fmt} is {daily_target}. "
                    f"Make {daily_target} dials today. Then again tomorrow. "
                    f"The math takes care of itself when you take care of the reps."
                ),
                (
                    f"Your {gci_goal_fmt} goal needs {daily_target} dials a day. {pace_line}\n\n"
                    f"Every great agent I've watched build a big year had one thing in common — "
                    f"when they saw a gap, they didn't look away. They looked straight at it and then went to work.\n\n"
                    f"Look straight at it today, {first}. Then pick up the phone."
                ),
            ]
        else:
            bodies = [
                (
                    f"#{rank} of {team_size} on the team yesterday.\n\n"
                    f"Team average: {team_avg} calls. {top_first} led with {top_calls}.\n\n"
                    f"The gap between last and first on this team isn't talent. It's reps. "
                    f"And you get a full set of reps today. Use them well, {first}."
                ),
            ]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 4: COMPOUND
# ---------------------------------------------------------------------------

def _arc_compound(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'Small daily actions nobody notices building to results everyone wants.'
    Best when: doing some activity but not seeing results, improving trend.
    """
    first    = ctx["first"]
    who      = ctx.get("who") or "the people you love"
    # Use the goal-derived daily call target when available — this is what the
    # GCI math is actually built on. ctx["daily_calls"] is the agent's own
    # self-reported number (stored in agent_profiles) and may be stale/wrong.
    calls_t  = goal_ctx["daily_target"] if goal_ctx else ctx.get("daily_calls", 20)
    gci_fmt  = ctx.get("gci_fmt") or "your goal"
    identity = ctx.get("identity") or "a top producer"
    has_goals = goal_ctx is not None

    goal_pre   = _goal_section(ctx, goal_ctx, tone)
    goal_block = f"{goal_pre}\n\n" if has_goals else ""

    days_remaining = max(1, (date(date.today().year, 12, 31) - date.today()).days)
    total_calls    = calls_t * days_remaining
    appt_rate      = 0.10
    total_appts    = round(total_calls * appt_rate)

    if tone == "funny":
        subjects = [
            f"What {calls_t} calls/day × {days_remaining} days actually looks like, {first}.",
            f"The compound effect is boring. Until it isn't. 📈",
            f"Nobody writes books about the agent who showed up every Tuesday. Except James Clear did.",
            f"Your reps right now are invisible. Until suddenly they're not. {first}.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"Let me give you the math that nobody talks about.\n\n"
                f"{calls_t} calls a day × {days_remaining} working days left = {total_calls:,} calls.\n"
                f"At a 10% calls-to-appointment rate, that's {total_appts} appointments.\n\n"
                f"You are not skipping a phone call today. "
                f"You are skipping {total_appts} appointments that could change your year.\n\n"
                f"Atomic Habits calls this the plateau of latent potential — it looks like nothing is happening, "
                f"then suddenly everything is. You're in the plateau. Keep going.\n\n"
                f"(The agents watching you right now have no idea what's about to hit.)"
            ),
            (
                f"{goal_block}"
                f"James Clear's most underrated line: 'You don't rise to the level of your goals, "
                f"you fall to the level of your systems.'\n\n"
                f"Your system is {calls_t} calls a day. That system, if you protect it, compounds "
                f"into something nobody expected — not even you.\n\n"
                f"The boring part is this week. The exciting part is Q4 when the leaderboard looks different "
                f"and people are asking what you did differently.\n\n"
                f"You'll say: I just made my calls. Every day. Even today.\n\n"
                f"Especially today, {first}."
            ),
            (
                f"{goal_block}"
                f"{calls_t} calls × {days_remaining} days = {total_calls:,} total calls. "
                f"At your conversion rate, that's a very different year-end number than zero calls today.\n\n"
                f"The compound effect doesn't reward intensity. It rewards consistency. "
                f"It doesn't care if you crushed it last week and coast this week. "
                f"It cares about today.\n\n"
                f"Today is a Tuesday that nobody will remember — except your future self, when {gci_fmt} is real.\n\n"
                f"(Never miss twice, {first}. That's the whole rule.)"
            ),
        ]
        if who:
            kicker = f"\n\nP.S. {who.capitalize()} are going to love Q4 you. Build them right now."
            bodies = [b + kicker for b in bodies]

    else:  # serious
        subjects = [
            f"{first}, the invisible work you're doing right now is building something.",
            f"Small daily actions nobody notices. Results everyone wants. That's you, {first}.",
            f"The plateau of latent potential is real, {first}. You're in it. Keep going.",
            f"The math of your year looks simple. Here's what it actually adds up to, {first}.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"Here's what {calls_t} calls a day actually builds:\n\n"
                f"{calls_t}/day × {days_remaining} working days left = {total_calls:,} total calls. "
                f"At 10% calls-to-appointment, that's {total_appts} shots at a closing.\n\n"
                f"That's not a number you manufacture in a sprint. That's a number you build one morning at a time.\n\n"
                f"James Clear calls it the plateau of latent potential — effort builds silently before results show loudly. "
                f"You're in the plateau right now, {first}. The work is invisible. "
                f"The results are coming.\n\nKeep going."
            ),
            (
                f"{goal_block}"
                f"The agents who close big years don't have a secret. They have a system.\n\n"
                f"Their system is unglamorous: {calls_t} calls a day. Every day. Logged. Repeated. "
                f"Nobody writes about it because it's not exciting. It just works.\n\n"
                f"You're building that system right now, {first}. Today's calls are the foundation that "
                f"future-you stands on when {gci_fmt} is real and the year looks like it was inevitable.\n\n"
                f"It wasn't inevitable. It was today. And yesterday. And tomorrow.\n\n"
                f"Make your calls."
            ),
            (
                f"{goal_block}"
                f"I want you to think about something.\n\n"
                f"The market doesn't know it's Wednesday. Your leads don't know you're tired. "
                f"Your goal doesn't know you had a hard week last week.\n\n"
                f"What they know is whether you showed up.\n\n"
                f"{calls_t} calls today stacks on top of everything you've already done. "
                f"That stack becomes {gci_fmt}. That changes things for {who}.\n\n"
                f"Today matters more than it looks like it does. Make {calls_t} calls, {first}."
            ),
        ]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 5: COMEBACK
# ---------------------------------------------------------------------------

def _arc_comeback(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'Every champion has this exact chapter.'
    Best when: streak broke, behind pace, declining trend.
    """
    first    = ctx["first"]
    who      = ctx.get("who") or "the people you love"
    gci_fmt  = ctx.get("gci_fmt") or "your goal"
    identity = ctx.get("identity") or "a top producer"
    calls_t  = ctx.get("daily_calls", 20)
    streak   = ctx.get("streak", 0)
    has_goals = goal_ctx is not None
    has_why   = situation.get("has_why")

    goal_pre   = _goal_section(ctx, goal_ctx, tone)
    goal_block = f"{goal_pre}\n\n" if has_goals else ""

    if tone == "funny":
        subjects = [
            f"The comeback arc just started, {first}. (It's a good one.) 🎬",
            f"Every great story has this chapter, {first}. Yours is called 'right now.'",
            f"Slumps happen to everyone. Except they end. Today is when yours ends. 💪",
            f"The team's favorite comeback story just got a new episode, {first}.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"I've been in this business since before Zillow was a thing. And I want to tell you something:\n\n"
                f"Every agent I've watched have a breakout year had a moment exactly like this one first.\n\n"
                f"A rough stretch. Numbers not where they should be. The streak broke. The vibe was off. "
                f"They thought about quitting — or at least dialing it back.\n\n"
                f"The ones who pushed through that moment? The ones who made the calls anyway? "
                f"They're the ones writing the story everyone else reads.\n\n"
                f"The agents who quit always do it right before the breakthrough. Don't be that agent.\n\n"
                f"Today is day 1 of the comeback. FUB is waiting. {who.capitalize()} are waiting. Let's go."
            ),
            (
                f"{goal_block}"
                f"Here's the thing about slumps that nobody tells you: they end exactly when you decide they do.\n\n"
                f"Not when the market improves. Not when better leads land in your inbox. "
                f"Not when you 'feel ready.' The slump ends when you pick up the phone.\n\n"
                f"James Clear would say: never miss twice. You had a rough patch. That's done now. "
                f"Today's calls restart the streak, restart the momentum, restart the story.\n\n"
                f"{calls_t} calls. New streak. Let's see what {identity} looks like on a comeback day."
            ),
            (
                f"{goal_block}"
                f"Real talk: I had $50K in credit card debt, two surgeries, a high-risk pregnancy, "
                f"and a market that dropped 40% in one year. All at once. 2008.\n\n"
                f"I didn't quit. I made my calls.\n\n"
                f"I'm not telling you your situation is that hard — maybe it is, maybe it isn't. "
                f"I'm telling you that the calls still work, the market is still moving, "
                f"and the agents who pick up the phone in the hard chapter are the ones who get to tell the good one.\n\n"
                f"Make {calls_t} calls today, {first}. This is the chapter that makes you."
            ),
        ]
        if has_why:
            kicker = f"\n\nP.S. {who.capitalize()} aren't rooting for you to give up in chapter 2."
            bodies = [b + kicker for b in bodies]

    else:  # serious
        subjects = [
            f"{first}, every champion has this exact chapter. This one's yours.",
            f"The agents who break through always push through this first, {first}.",
            f"This is the part of the story that matters most, {first}. Right here.",
            f"The comeback starts with one call, {first}. Today.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"The agents who quit always do it right before the breakthrough.\n\n"
                f"Not because it was hopeless. Because it finally got hard enough to show what they were made of.\n\n"
                f"You're in that moment right now, {first}. Not because you're failing — because you're in the chapter "
                f"that every great agent has in their story. The one that separates the ones who stay from the ones who don't.\n\n"
                f"{identity} stays. {identity} makes {calls_t} calls today and doesn't look back.\n\n"
                f"This is that morning. Let's go."
            ),
            (
                f"{goal_block}"
                f"I want to tell you what I know about comebacks.\n\n"
                f"They don't look dramatic when they start. They look like one ordinary morning "
                f"where you decided to do the thing you knew you should do.\n\n"
                f"Today is that morning for you, {first}. {gci_fmt} is still possible. "
                f"Your leads are still there. The market is still moving. "
                f"The only thing that was missing was the decision.\n\n"
                f"You've made it. Now make {calls_t} calls."
            ),
            (
                f"{goal_block}"
                f"The valley of disappointment is the most honest part of Atomic Habits.\n\n"
                f"Effort builds before results show. You do the work, the graph looks flat, "
                f"you wonder if it's working — and then it suddenly isn't flat anymore.\n\n"
                f"You're in the valley. Every agent worth knowing has stood exactly where you're standing. "
                f"The ones who made it out didn't wait for motivation — they made the calls and let the results follow.\n\n"
                f"Make {calls_t} calls today. The valley has an exit and you just found it."
            ),
        ]
        if has_why:
            why_close = f"\n\nFor {who}. For what you said you were building. Today is the start of that being real."
            bodies = [b + why_close for b in bodies]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 6: ELITE STANDARD
# ---------------------------------------------------------------------------

def _arc_elite(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'What does #1 on this specific team actually do?'
    Best when: bottom half, last place, closer/competitive archetype.
    """
    first    = ctx["first"]
    who      = ctx.get("who") or "the people you love"
    gci_fmt  = ctx.get("gci_fmt") or "your goal"
    identity = ctx.get("identity") or "a top producer"
    calls_t  = ctx.get("daily_calls", 20)
    rank     = situation.get("rank", 1)
    team_size = situation.get("team_size", 1)
    top_first = top_agent["name"].split()[0] if top_agent else "the top agent"
    top_calls = top_agent["calls"] if top_agent else team_avg
    gap_calls = max(0, top_calls - (calls_t // 4))  # approximate gap based on target
    has_goals = goal_ctx is not None

    goal_pre   = _goal_section(ctx, goal_ctx, tone)
    goal_block = f"{goal_pre}\n\n" if has_goals else ""

    days_to_close = max(1, round(gap_calls / max(calls_t, 1)))

    if tone == "funny":
        subjects = [
            f"{first}, the top agent made {top_calls} calls. You had... less. Let's talk. 😅",
            f"#{rank} of {team_size}. {top_first}'s not worried. Yet. 👀",
            f"The scoreboard has opinions about {first}'s call volume. So do I.",
            f"{top_first} called {top_calls} times yesterday. What was YOUR excuse? 🤷",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"Let me tell you what the top agent on this team actually does.\n\n"
                f"{top_first} made {top_calls} calls {day_name}. "
                f"The gap between last place and first place on this team right now? It's not talent. It's not zip code. "
                f"It's not magic.\n\nIt's {gap_calls} more dials a day.\n\n"
                f"You can close that gap in {days_to_close} days. Not by being brilliant — by being consistent. "
                f"The agents who are being too nice to their call list lose this competition every time.\n\n"
                f"Don't be too nice to your call list today, {first}. Make your dials."
            ),
            (
                f"{goal_block}"
                f"I'll be honest — I check the leaderboard every morning. And I ask myself: "
                f"who's deciding to be different today?\n\n"
                f"Right now {top_first} is at the top. {top_calls} calls {day_name}. "
                f"Elite standard isn't complicated — it's {top_calls} calls. "
                f"It's showing up on days when the team doesn't.\n\n"
                f"You've got the same 24 hours. The same leads. The same phone.\n\n"
                f"#{rank} of {team_size} isn't {identity}. Start climbing. Today."
            ),
            (
                f"{goal_block}"
                f"Here's the competitive math nobody talks about:\n\n"
                f"Team average yesterday: {team_avg} calls. Top performer: {top_calls}. "
                f"The difference between {top_first} and average isn't hustle — it's {top_calls - team_avg} extra dials a day.\n\n"
                f"Three days of matching {top_first}'s number and you're a different agent on the leaderboard.\n\n"
                f"What does #{rank} of {team_size} feel like? What would #1 feel like?\n\n"
                f"Today, you get to choose. {calls_t} calls, minimum. Let's see it."
            ),
        ]

    else:  # serious
        subjects = [
            f"Elite standard on this team: here's the specific number, {first}.",
            f"The gap between #{rank} and #1 isn't talent. Here's what it actually is, {first}.",
            f"What the top agent on this team actually does, {first}.",
            f"{first}, {identity} belongs at the top. Here's the path.",
        ]
        subject = random.choice(subjects)

        bodies = [
            (
                f"{goal_block}"
                f"The top agent on this team made {top_calls} calls {day_name}.\n\n"
                f"You're currently at #{rank} of {team_size}. The gap between last and first isn't talent. "
                f"It isn't market knowledge. It isn't luck.\n\n"
                f"It's {gap_calls} more dials per day. You can close that in {days_to_close} days. "
                f"Not by being brilliant — just by being consistent in a way most agents on this team aren't willing to be.\n\n"
                f"Are you going to be too nice to your discomfort today, {first}? "
                f"Or are you going to make {calls_t} calls and start closing the gap?"
            ),
            (
                f"{goal_block}"
                f"Let me be specific because specifics are what actually change behavior.\n\n"
                f"{top_first} led this team {day_name} with {top_calls} calls. "
                f"Team average: {team_avg}. You're at #{rank}.\n\n"
                f"Elite on this team is a defined number. It's {top_calls} calls. "
                f"You know what's between you and that number? Just reps.\n\n"
                f"Start today, {first}. {calls_t} calls minimum. Every day you do this, "
                f"the leaderboard looks different — and so does {gci_fmt}."
            ),
            (
                f"{goal_block}"
                f"I've watched top producers on this team build their results from the bottom of the leaderboard.\n\n"
                f"Every single one of them had a day exactly like today — where the gap felt real and the number felt far. "
                f"And they made their calls anyway.\n\n"
                f"Not because they weren't scared. Because they decided that {who} was worth the discomfort.\n\n"
                f"Make {calls_t} calls today. Then match {top_first}'s {top_calls} before the week is out. "
                f"That's {identity} taking over. Let's see it."
            ),
        ]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Arc 7: DEAL MATH
# ---------------------------------------------------------------------------

def _arc_deal_math(ctx, situation, goal_ctx, deal_summary, tone, top_agent, team_avg, day_name):
    """
    'Every dial has a dollar sign on it.'
    Best when: agent has closed deals, goal is set, Q3-Q4.
    """
    first    = ctx["first"]
    who      = ctx.get("who") or "the people you love"
    calls_t  = ctx.get("daily_calls", 20)
    has_goals = goal_ctx is not None

    closings_ytd = int((deal_summary or {}).get("closings", 0))
    gci_ytd      = float((deal_summary or {}).get("gci_est", 0.0))

    # Build deal math from goals
    if has_goals:
        gci_goal     = float(goal_ctx["gci_goal"])
        gci_goal_fmt = goal_ctx["gci_fmt"]
        avg_price    = float(goal_ctx.get("avg_sale_price", 0)) or 400000  # fallback
        # Derive avg commission from deal summary if we have closings
        if closings_ytd > 0 and gci_ytd > 0:
            avg_commission = round(gci_ytd / closings_ytd)
        else:
            # Estimate from goal
            avg_commission = round(gci_goal * 0.067) if gci_goal else 9600  # ~$150K/14 deals
        closings_needed_total = max(1, round(gci_goal / avg_commission)) if avg_commission > 0 else 14
        closings_remaining    = max(0, closings_needed_total - closings_ytd)
        gci_remaining         = max(0.0, gci_goal - gci_ytd)
    else:
        gci_goal_fmt       = _fmt_gci(ctx.get("gci_goal", 0))
        avg_commission     = 9600
        closings_needed_total = 14
        closings_remaining    = max(0, 14 - closings_ytd)
        gci_remaining         = 0.0

    avg_commission_fmt = _fmt_gci(avg_commission)
    gci_remaining_fmt  = _fmt_gci(gci_remaining)

    if tone == "funny":
        subjects = [
            f"Every call you make today has a dollar sign on it, {first}. Here's the math.",
            f"{first}, let's talk about what {closings_remaining} more closings looks like. 💰",
            f"Your average deal: {avg_commission_fmt}. Your calls today: 0 so far. Math check. 🧮",
            f"The invisible price tag on your call list, {first}.",
        ]
        subject = random.choice(subjects)

        if closings_ytd > 0:
            bodies = [
                (
                    f"Let's make the invisible visible.\n\n"
                    f"You've closed {closings_ytd} deal{'s' if closings_ytd != 1 else ''} for {_fmt_gci(gci_ytd)} GCI. "
                    f"Your average commission: {avg_commission_fmt}.\n\n"
                    f"To hit {gci_goal_fmt}, you need {closings_remaining} more closing{'s' if closings_remaining != 1 else ''}. "
                    f"That's {gci_remaining_fmt} sitting in your pipeline right now — attached to leads you haven't closed yet.\n\n"
                    f"Every dial today has {avg_commission_fmt} on the line. Not in theory. In actual math.\n\n"
                    f"Are you being too nice to your call list right now? Because the math disagrees. Go dial."
                ),
                (
                    f"You've got {closings_remaining} more closing{'s' if closings_remaining != 1 else ''} standing between you and {gci_goal_fmt}.\n\n"
                    f"Each one is worth {avg_commission_fmt} to you personally. "
                    f"{gci_remaining_fmt} total. That's not abstract. That's real.\n\n"
                    f"The agents who hit {gci_goal_fmt} made their calls on the days that felt optional. "
                    f"Today feels optional. It isn't. {calls_t} calls. Let's go find one of those {closings_remaining}."
                ),
                (
                    f"Quick math:\n\n"
                    f"YTD: {closings_ytd} closing{'s' if closings_ytd != 1 else ''} = {_fmt_gci(gci_ytd)} GCI.\n"
                    f"Still needed: {closings_remaining} more = {gci_remaining_fmt}.\n"
                    f"Your average deal: {avg_commission_fmt}.\n\n"
                    f"The math doesn't care what day it is. It just counts. "
                    f"Every call today is a step toward one of those {closings_remaining}.\n\n"
                    f"(Seriously — someone in your database is ready to move. They're just waiting for your call.)"
                ),
            ]
        else:
            bodies = [
                (
                    f"Let's talk about what your first closing unlocks.\n\n"
                    f"Your average deal: {avg_commission_fmt}. "
                    f"Goal: {gci_goal_fmt}. Closings needed: {closings_needed_total}.\n\n"
                    f"The first one is the hardest — not because the market is harder, but because you haven't seen the math work for you yet. "
                    f"Once you do, the whole picture changes.\n\n"
                    f"Every call today is building toward that first number on the board. "
                    f"Make {calls_t} of them. The math is waiting.\n\n"
                    f"(Hint: the first closing is always hiding in a conversation you almost didn't have.)"
                ),
                (
                    f"Zero on the board right now doesn't mean zero at year-end.\n\n"
                    f"Here's what the math looks like when it starts moving:\n"
                    f"1 closing = {avg_commission_fmt}.\n"
                    f"5 closings = {_fmt_gci(avg_commission * 5)}.\n"
                    f"Goal of {closings_needed_total} closings = {gci_goal_fmt}.\n\n"
                    f"None of that happens without the call. Make {calls_t} of them today. "
                    f"Get the first one on the board."
                ),
            ]

    else:  # serious
        subjects = [
            f"{first}, here's the exact math between where you are and {gci_goal_fmt}.",
            f"Your deal math is specific, {first}. Here's what it says.",
            f"Every dial today has a dollar sign on it, {first}. Here's what that number is.",
            f"{closings_remaining} more closings. {gci_remaining_fmt}. Here's what that looks like, {first}.",
        ]
        subject = random.choice(subjects)

        if closings_ytd > 0:
            bodies = [
                (
                    f"Here's where your year actually stands:\n\n"
                    f"Closings YTD: {closings_ytd}. GCI earned: {_fmt_gci(gci_ytd)}.\n"
                    f"Closings still needed to hit {gci_goal_fmt}: {closings_remaining}.\n"
                    f"Average commission per deal: {avg_commission_fmt}.\n"
                    f"GCI remaining to earn: {gci_remaining_fmt}.\n\n"
                    f"The gap is real. It's also specific — which means it has a specific solution.\n\n"
                    f"That solution is {calls_t} calls a day, consistently, for the rest of the year. "
                    f"Each call today moves the needle on those {closings_remaining} closings. "
                    f"Make them, {first}."
                ),
                (
                    f"You've closed {closings_ytd} deal{'s' if closings_ytd != 1 else ''} this year. "
                    f"That's {_fmt_gci(gci_ytd)} earned — and {closings_remaining} more needed to hit {gci_goal_fmt}.\n\n"
                    f"Your average commission is {avg_commission_fmt}. "
                    f"Every conversation you have today has that number attached to it in some form — "
                    f"directly or down the referral chain.\n\n"
                    f"The agents who hit their goal don't think about the number at year-end. "
                    f"They think about the {calls_t} calls in front of them today. Do the same, {first}."
                ),
            ]
        else:
            bodies = [
                (
                    f"Let me make the math clear.\n\n"
                    f"Your goal is {gci_goal_fmt}. Your average commission per deal: {avg_commission_fmt}. "
                    f"Closings needed: {closings_needed_total}.\n\n"
                    f"Your first closing changes the whole picture — not just financially, but psychologically. "
                    f"Once you've proven the math works for you, the rest of the year moves differently.\n\n"
                    f"The path to that first closing runs through today's {calls_t} calls. "
                    f"The conversation that starts the chain is in your database right now. "
                    f"Go find it."
                ),
                (
                    f"You haven't closed one yet this year. That's data, not destiny.\n\n"
                    f"To hit {gci_goal_fmt}, you need {closings_needed_total} closings at {avg_commission_fmt} each. "
                    f"The first one is the breakthrough — every one after it gets easier because you've seen the math work.\n\n"
                    f"Today's {calls_t} calls are the most important you've made this year. "
                    f"Because somewhere in that list is the conversation that starts everything.\n\n"
                    f"Make them, {first}. For {who} and for the math."
                ),
            ]

    return subject, random.choice(bodies)


# ---------------------------------------------------------------------------
# Closing milestone email (called when a new closing is detected)
# ---------------------------------------------------------------------------

def build_closing_milestone_email(ctx, deal, deal_summary, goal_ctx):
    """
    Build a (subject, body) celebration email for a new closing.

    ctx         : agent context dict from nudge_engine._ctx()
    deal        : dict with deal_name, gci_estimated, close_date
    deal_summary: dict from db.get_deal_summary()
    goal_ctx    : output of _build_goal_ctx() or None

    Returns (subject, body) tuple.
    """
    first        = ctx["first"]
    who          = ctx.get("who") or "the people you love"
    gci_fmt      = ctx.get("gci_fmt") or "your goal"
    closings_ytd = int((deal_summary or {}).get("closings", 0))
    gci_ytd      = float((deal_summary or {}).get("gci_est", 0.0))
    deal_gci     = float((deal or {}).get("gci_estimated", 0))
    deal_name    = (deal or {}).get("deal_name", "a new deal")
    has_goals    = goal_ctx is not None

    if has_goals:
        gci_goal_fmt  = goal_ctx["gci_fmt"]
        gci_goal      = float(goal_ctx.get("gci_goal", 0))
        closings_ytd_g = closings_ytd
        avg_commission = round(deal_gci) if deal_gci > 0 else round(gci_ytd / max(closings_ytd, 1))
        closings_needed_total = max(1, round(gci_goal / max(avg_commission, 1)))
        closings_remaining    = max(0, closings_needed_total - closings_ytd)
        gci_remaining         = max(0.0, gci_goal - gci_ytd)
        remaining_line = (
            f"{closings_remaining} more closing{'s' if closings_remaining != 1 else ''} "
            f"to hit {gci_goal_fmt}. {_fmt_gci(gci_remaining)} left to earn."
        )
    else:
        gci_goal_fmt   = gci_fmt
        closings_remaining = None
        remaining_line = f"Keep going — the year's not over."

    deal_gci_fmt = _fmt_gci(deal_gci) if deal_gci else ""

    # First closing of the year gets a special opener
    is_first_close = closings_ytd == 1

    tone = random.choice(["warm", "fired_up"])  # Milestone emails use warm or fired_up — not sassy

    if is_first_close:
        if tone == "warm":
            subjects = [
                f"First one on the board, {first}. {deal_name} is closed. 🏡",
                f"You're on the board, {first}. Closing #1 is done.",
                f"One down, {first}. Here's what the math looks like now.",
            ]
            subject = random.choice(subjects)
            body = random.choice([
                (
                    f"The first one is always the hardest — not because the work is different, "
                    f"but because you haven't seen the machine work for you yet.\n\n"
                    f"Now you have.\n\n"
                    f"{deal_name} is closed. "
                    + (f"{deal_gci_fmt} earned. " if deal_gci_fmt else "")
                    + f"That's real.\n\n"
                    f"{remaining_line}\n\n"
                    f"The first closing changes the year psychologically. "
                    f"Everything after this is just repeating what you just proved you can do.\n\n"
                    f"Go celebrate — then call your next lead. The math is working, {first}."
                ),
                (
                    f"1 down.\n\n"
                    + (f"{closings_remaining} to go.\n\n" if closings_remaining is not None else "Keep going.\n\n")
                    + f"You just proved the whole system works — the calls, the conversations, the follow-up, all of it. "
                    f"{deal_name} is closed and {deal_gci_fmt + ' is yours' if deal_gci_fmt else 'the commission is yours'}.\n\n"
                    f"Now here's what I want you to do: enjoy it for exactly one day. "
                    f"Then get back on the phone, because {who} doesn't want a one-closing year.\n\n"
                    f"Neither do you. Go get the next one, {first}."
                ),
            ])
        else:  # fired_up
            subjects = [
                f"FIRST CLOSING DOWN! 🏡 {first}, you're on the board!",
                f"{first} is on the board! {deal_name} — CLOSED.",
                f"One closing in the books, {first}. The math is officially working.",
            ]
            subject = random.choice(subjects)
            body = (
                f"FIRST ONE DOWN, {first}!\n\n"
                f"{deal_name} is closed. "
                + (f"That's {deal_gci_fmt} earned. " if deal_gci_fmt else "")
                + f"That's the whole machine proving it works for YOU.\n\n"
                f"{remaining_line}\n\n"
                f"You made the calls. You had the conversations. You followed up when it was easier not to. "
                f"This closing is the proof that all of that is real.\n\n"
                f"Go tell {who}. Then come back tomorrow and do it again.\n\n"
                f"This year just got real, {first}. Let's go."
            )
    else:
        ordinal_map = {2: "2nd", 3: "3rd", 4: "4th", 5: "5th", 6: "6th", 7: "7th",
                       8: "8th", 9: "9th", 10: "10th"}
        ordinal = ordinal_map.get(closings_ytd, f"{closings_ytd}th")

        if tone == "warm":
            subjects = [
                f"Closing #{closings_ytd} down, {first}. Here's what the math looks like now.",
                f"{ordinal} closing, {first}. {deal_name} — done.",
                f"Another one on the board, {first}. Keep stacking.",
            ]
            subject = random.choice(subjects)
            body = random.choice([
                (
                    f"{ordinal} closing of the year — {deal_name} is done.\n\n"
                    + (f"That's {deal_gci_fmt} more GCI. " if deal_gci_fmt else "")
                    + f"YTD: {closings_ytd} closing{'s' if closings_ytd != 1 else ''}, {_fmt_gci(gci_ytd)} earned.\n\n"
                    f"{remaining_line}\n\n"
                    f"This is the compound effect made visible. Every call that felt optional wasn't. "
                    f"Every follow-up that felt unnecessary mattered. This closing is the proof.\n\n"
                    f"Celebrate it. Then call your next lead. "
                    f"The year isn't done and neither are you, {first}."
                ),
                (
                    f"Closing #{closings_ytd} is on the board.\n\n"
                    + (f"{deal_gci_fmt} earned on {deal_name}. " if deal_gci_fmt else "")
                    + f"Here's what the year looks like right now:\n\n"
                    + f"Closings YTD: {closings_ytd}. GCI earned: {_fmt_gci(gci_ytd)}.\n"
                    + f"{remaining_line}\n\n"
                    + f"You're building something real, {first}. "
                    + f"Each closing isn't just commission — it's proof to {who} that the decision to go all in was right.\n\n"
                    + f"Keep going."
                ),
            ])
        else:  # fired_up
            subjects = [
                f"#{closings_ytd} on the year! {first}, let's GO! 🏠",
                f"Closing #{closings_ytd} — {deal_name}. Stack it up, {first}.",
                f"{ordinal} closing of the year, {first}. The math keeps working.",
            ]
            subject = random.choice(subjects)
            body = (
                f"Closing #{closings_ytd}!\n\n"
                f"{deal_name} — DONE. "
                + (f"{deal_gci_fmt} more GCI. " if deal_gci_fmt else "")
                + f"YTD: {_fmt_gci(gci_ytd)} earned.\n\n"
                f"{remaining_line}\n\n"
                f"The agents who stack closings like this don't do it by accident. "
                f"They make the calls. They follow up. They don't quit when it gets uncomfortable.\n\n"
                f"That's you, {first}. Now go celebrate with {who} — "
                f"then get back on the phone. The next closing is in your database right now."
            )

    return subject, body
