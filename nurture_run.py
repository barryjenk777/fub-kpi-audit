"""
nurture_run.py — The Nurture Run: weekly one-tap nurture for agents.

The nurture problem is composition cost. Every touch used to require the
agent to remember the lead, invent a message, and type it. This engine
removes all three: each Tuesday every agent gets an email of lead cards,
each with a why-now line, a pre-written text in Barry's voice built from
the lead's own city data and behavior, and ONE button that opens their
Messages app pre-addressed with the text composed. They read, tap, send
from their own number. Ten minutes clears a week of nurture.

Verification is the moat: the tap is logged at the redirect, and the FUB
outbound webhook stamps the card verified when the real text or call
lands. The scoreboard feeds Pulse and the Sunday recap, and the proof of
life is Maverick's nurture past-due backlog falling week over week.

Sources, in priority order: Maverick's own overdue flags (the OOC ledger,
oldest and hottest stages first), enriched from FUB at compose time.
Division of labor stands: the AI nurtures the pond and the true-cold;
agents run their assigned hot and warm. Deterministic except the message
banks, which come from the market snapshots (already generated and
sanitized: no dashes, one number per text, never "just checking in").
"""

import logging
import os
import secrets
from datetime import date, datetime, timedelta, timezone

import config
import db as _db
from fub_client import FUBClient

logger = logging.getLogger(__name__)

_EXCLUDED = set(getattr(config, "EXCLUDED_USERS", [])) \
    | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))

CARDS_PER_AGENT = 10

# Stage priority: closest to money first. Matched as lowercase substrings
# against the FUB stage name.
_STAGE_RANK = [
    ("actively listed", 0), ("showing", 0),
    ("appointment", 1), ("hot", 2),
    ("short term", 3), ("warm", 3),
    ("long term", 4), ("nurture", 4),
]

_MP_CITIES = {
    "virginia beach": "virginia-beach", "norfolk": "norfolk",
    "chesapeake": "chesapeake", "suffolk": "suffolk",
    "portsmouth": "portsmouth", "hampton": "hampton",
    "newport news": "newport-news",
}

# Evergreen texts for leads whose city has no snapshot. Barry voice: value
# or a real question, one idea, no dashes, never "checking in".
_EVERGREEN_TEXTS = [
    ("Hey {first}, a home hit my radar this morning that made me think of "
     "your search. Before I send it over, has anything changed about what "
     "you are looking for?"),
    ("{first}, quick question I ask everyone I work with this time of "
     "year: if the right house showed up next weekend, would you want to "
     "see it, or is the timing still off? Either answer helps me help you."),
    ("{first}, I was reviewing my list today and realized I owe you an "
     "update on the market. Want the short version by text, or is a five "
     "minute call easier?"),
    ("Hey {first}, no agenda here. I keep a small list of people I check "
     "on because the market moves whether we watch it or not. What is the "
     "one thing that would make a move worth it for you?"),
]

_EVERGREEN_CALL = (
    "{first}, this is a quick one, I promise. I was going through my "
    "notes and you crossed my mind. Last we talked you were thinking "
    "about {topic}. How is that sitting with you these days?")


def _stage_rank(stage):
    s = (stage or "").lower()
    for frag, rank in _STAGE_RANK:
        if frag in s:
            return rank
    return 5


def _lead_side(person):
    probe = " ".join(person.get("tags") or []).lower() + " " + \
        str(person.get("stage") or "").lower()
    return "sellers" if "sell" in probe else "buyers"


def _lead_city_slug(person):
    blob = " ".join(a.get("formatted", "") if isinstance(a, dict) else str(a)
                    for a in (person.get("addresses") or [])).lower()
    blob += " " + " ".join(person.get("tags") or []).lower()
    for city, slug in _MP_CITIES.items():
        if city in blob:
            return slug
    return None


def _first(name):
    return (name or "").split()[0] if name else "there"


def _compose_message(person, flag_age_days):
    """One ready-to-send text, personalized. City snapshot bank first,
    evergreen fallback. Returns (message, why_line, call_line)."""
    first = _first(person.get("name"))
    side = _lead_side(person)
    slug = _lead_city_slug(person)
    stage = (person.get("stage") or "").strip()

    why_bits = []
    if stage:
        why_bits.append(stage)
    if flag_age_days is not None and flag_age_days > 0:
        why_bits.append("%d days since a real touch" % flag_age_days)
    why = " · ".join(why_bits) or "on your list"

    message, call_line, variant = None, None, None
    if slug:
        snap = _db.get_market_snapshot(slug) or {}
        nurture = (snap.get("nurture") or {}).get(side) or {}
        texts = nurture.get("texts") or []
        if texts:
            pid_digits = int("".join(c for c in str(person.get("id") or "0")
                                     if c.isdigit()) or 0)
            week = date.today().isocalendar()[1]
            idx = (pid_digits + week) % len(texts)
            pick = texts[idx]
            message = pick.replace("{first}", first)
            variant = "%s:%s:%d" % (slug, side, idx)
        if nurture.get("call_track"):
            call_line = nurture["call_track"].replace("{first}", first)
    if not message:
        pid_digits = int("".join(c for c in str(person.get("id") or "0")
                                 if c.isdigit()) or 0)
        idx = pid_digits % len(_EVERGREEN_TEXTS)
        pick = _EVERGREEN_TEXTS[idx]
        message = pick.replace("{first}", first)
        variant = "evergreen:%s:%d" % (side, idx)
    if not call_line:
        topic = "your next move" if side == "buyers" else "what your place is worth"
        call_line = _EVERGREEN_CALL.replace("{first}", first).replace("{topic}", topic)
    return message, why, call_line, variant


def _best_phone(person):
    for p in (person.get("phones") or []):
        v = (p.get("value") or "").strip() if isinstance(p, dict) else str(p)
        if v:
            return v
    return None


def _pick_leads(agent_name, client, recent_hotsheet_pids):
    """Maverick's overdue flags for this agent, hottest stage then oldest
    flag, enriched from FUB. Skips leads the morning text already named
    this week (one voice per lead per week)."""
    flags = _db.get_ooc_open(agent_name=agent_name, min_age_days=0, limit=80)
    cards = []
    for f in flags:
        pid = str(f["person_id"])
        if pid in recent_hotsheet_pids:
            continue
        try:
            person = client.get_person(pid)
        except Exception:
            continue
        if not person or not (person.get("name") or "").strip():
            continue
        phone = _best_phone(person)
        if not phone:
            continue
        tags_l = [t.lower() for t in (person.get("tags") or [])]
        if "sms_optout" in tags_l or "bad_number" in tags_l:
            continue
        cards.append({"person": person, "age": f.get("age_days"),
                      "rank": _stage_rank(person.get("stage")),
                      "phone": phone})
        if len(cards) >= CARDS_PER_AGENT * 2:
            break
    cards.sort(key=lambda c: (c["rank"], -(c["age"] or 0)))
    return cards[:CARDS_PER_AGENT]


# ── Email ───────────────────────────────────────────────────────────────────

def _card_html(base, token, lead_name, why, message, fub_url):
    first = _first(lead_name)
    msg_html = message.replace("&", "&amp;").replace("<", "&lt;")
    return """
<tr><td style="padding:0 28px 14px">
  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0"
         style="background:#ffffff;border:1px solid #e8e6df;border-radius:12px">
    <tr><td style="padding:16px 18px 4px">
      <span style="font-size:16px;font-weight:800;color:#1a1f26">%s</span>
      <span style="font-size:12px;color:#8a8f98">&nbsp; %s</span>
    </td></tr>
    <tr><td style="padding:8px 18px 12px">
      <div style="background:#fffbf0;border-left:4px solid #f5a623;border-radius:6px;
                  padding:10px 14px;font-size:14px;line-height:1.55;color:#3d4450;
                  font-style:italic">%s</div>
    </td></tr>
    <tr><td style="padding:0 18px 16px">
      <a href="%s/nr/%s/go" style="display:inline-block;background:#f5a623;
         color:#0d1117;font-size:15px;font-weight:800;text-decoration:none;
         border-radius:10px;padding:12px 22px">Text %s from FUB &rarr;</a>
      <a href="%s/nr/%s/fub" style="display:inline-block;font-size:12px;
         color:#8a8f98;text-decoration:underline;padding:12px 10px">record only</a>
    </td></tr>
  </table>
</td></tr>""" % (lead_name, why, msg_html, base, token, first, base, token)


def build_email(agent_first, cards_html, n_cards, call_line, scoreboard_line):
    minutes = max(5, n_cards)
    sb = ("<div style='font-size:13px;color:#c7cdd6;margin-top:10px'>%s</div>"
          % scoreboard_line) if scoreboard_line else ""
    return """<div style="background:#f4f4f0;padding:26px 0">
<table role="presentation" width="100%%" cellpadding="0" cellspacing="0"><tr><td align="center">
<table role="presentation" width="560" cellpadding="0" cellspacing="0"
       style="max-width:560px;width:100%%;font-family:-apple-system,'Segoe UI',Arial,sans-serif">
<tr><td style="background:#0d1117;padding:26px 30px">
  <div style="font-size:11px;font-weight:800;letter-spacing:.2em;color:#f5a623;
              text-transform:uppercase;margin-bottom:8px">The Nurture Run</div>
  <div style="font-size:26px;font-weight:900;color:#ffffff;line-height:1.15">
    %s, %d of your leads need to hear from you.</div>
  <div style="font-size:15px;color:#c7cdd6;margin-top:8px">About %d minutes,
    phone in hand. Every message below is already written. Tap the button,
    it copies the message and opens the lead in FUB. Paste, send from your
    FUB number, next card. Two taps per lead.</div>%s
</td></tr>
<tr><td style="height:4px;background:#f5a623;font-size:0">&nbsp;</td></tr>
<tr><td style="padding:22px 28px 6px;font-size:14px;line-height:1.6;color:#1a1f26;background:#f4f4f0">
  Why these people: every one of them raised their hand once, and every one
  has gone quiet on us. The agent who shows up with something worth reading
  is the agent they call when they are ready. Today that agent is you, and
  it costs you ten minutes.
</td></tr>
%s
<tr><td style="padding:8px 28px 18px;background:#f4f4f0">
  <table role="presentation" width="100%%" cellpadding="0" cellspacing="0"
         style="background:#0d1117;border-radius:12px">
    <tr><td style="padding:16px 18px">
      <div style="font-size:11px;font-weight:800;letter-spacing:.14em;
                  color:#f5a623;text-transform:uppercase;margin-bottom:6px">
        Rather call? 20 seconds, any of them</div>
      <div style="font-size:14px;font-style:italic;line-height:1.6;color:#e8edf8">%s</div>
    </td></tr>
  </table>
</td></tr>
<tr><td style="padding:4px 28px 26px;font-size:14px;line-height:1.65;color:#1a1f26;background:#f4f4f0">
  I see every send on my board, and so does the market: leads we touch stay
  ours, leads we ignore buy with somebody else. Nobody is asking you to
  write anything. I wrote it. You tap send.<br><br>
  Let's get it,<br><strong>Barry Jenkins</strong><br><br>
  <a href="https://www.legacycommandcenter.com/rhythm" style="font-size:12px;
     color:#8a8f98">Your full weekly rhythm, one page &rarr;</a>
</td></tr>
</table></td></tr></table></div>""" % (agent_first, n_cards, minutes, sb, cards_html, call_line)


def run_nurture_run(dry_run=True, only_agent=None, preview_to=None):
    """Build and (unless dry_run) send this week's Nurture Run.

    preview_to: when set, every email goes ONLY to this address (Barry's
    review path) with the agent's name in the subject."""
    import postmark_client as _pm
    client = FUBClient()
    base = (os.environ.get("BASE_URL")
            or "https://web-production-3363cc.up.railway.app").rstrip("/")
    run_date = date.today()
    profiles = [p for p in (_db.get_agent_profiles(active_only=True) or [])
                if p["agent_name"] not in _EXCLUDED
                and (not only_agent or p["agent_name"] == only_agent)]

    recent = set()
    try:
        for pick in (_db.get_recent_hotsheet_pids(days=3) or []):
            recent.add(str(pick))
    except Exception:
        pass

    sb = {r["agent"]: r for r in (_db.nurture_scoreboard(weeks=2) or [])}
    summary = {"run_date": str(run_date), "dry_run": dry_run, "agents": {},
               "sent": 0}
    fub_base = "https://yourfriendlyagent.followupboss.com/2/people/view/%s"

    for p in profiles:
        agent = p["agent_name"]
        email = p.get("email")
        cards = _pick_leads(agent, client, recent)
        if not cards:
            summary["agents"][agent] = {"cards": 0, "skipped": "no due leads"}
            continue
        cards_html, saved = [], 0
        call_line = None
        for c in cards:
            person = c["person"]
            message, why, cl, variant = _compose_message(person, c.get("age"))
            call_line = call_line or cl
            token = secrets.token_urlsafe(9)
            if not dry_run:
                stored = _db.save_nurture_card(
                    run_date, token, agent, person.get("id"),
                    (person.get("name") or "").strip(), c["phone"], message, why,
                    variant=variant)
                if not stored:
                    continue
                token = stored  # same-day re-runs reuse the existing token
                try:
                    _db.log_variant("nurture_run", variant or "unknown",
                                    person.get("id"), agent)
                except Exception:
                    pass
            saved += 1
            cards_html.append(_card_html(
                base, token, (person.get("name") or "").strip(), why, message,
                fub_base % person.get("id")))
        if not cards_html:
            summary["agents"][agent] = {"cards": 0, "skipped": "nothing saved"}
            continue
        prev = sb.get(agent)
        sb_line = None
        if prev and prev.get("cards"):
            sb_line = ("Last run you handled %d of %d. %s"
                       % (prev["verified"], prev["cards"],
                          "Keep that up." if prev["verified"] >= prev["cards"] * 0.7
                          else "Let's beat it this week."))
        html = build_email(_first(agent), "".join(cards_html), saved,
                           call_line or "", sb_line)
        subject = ("%d of your leads need to hear from you "
                   "(all written, just tap send)" % saved)
        to = preview_to or email
        if preview_to:
            subject = "[PREVIEW %s] %s" % (agent, subject)
        summary["agents"][agent] = {"cards": saved, "to": to}
        if dry_run or not to:
            continue
        try:
            _pm.send(to="%s <%s>" % (agent, to), from_email=config.EMAIL_FROM,
                     subject=subject, html=html)
            summary["sent"] += 1
            if not preview_to:
                try:
                    _db.log_attention(agent, "nurture_run", "email")
                except Exception:
                    pass
        except Exception as e:
            logger.warning("[NURTURE RUN] send failed for %s: %s", agent, e)
            summary["agents"][agent]["error"] = str(e)[:120]
    return summary
