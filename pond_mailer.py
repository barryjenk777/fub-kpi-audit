"""
Pond Mailer — LeadStream Email Marketing Engine

Sends behaviorally-personalized emails to Shark Tank pond leads based on
their actual IDX activity: which properties they viewed, which they saved,
price range drift, and behavioral intent signals from Ylopo tags.

Each email is written by Claude based on the lead's real behavior —
not a template with merge tags. Every email is unique.

Usage:
    python pond_mailer.py                  # Dry run (no emails sent)
    python pond_mailer.py --apply          # Send live emails
    python pond_mailer.py --person 105456  # Single lead (by FUB person ID)
    python pond_mailer.py --limit 10       # Process first N leads only

Requirements:
    ANTHROPIC_API_KEY — set in Railway environment variables
    SENDGRID_API_KEY  — already set
    FUB_API_KEY       — already set
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] pond_mailer: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pond_mailer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# How many days between emails to the same lead
EMAIL_COOLDOWN_DAYS = 5

# Minimum IDX events needed to write a meaningful email
MIN_EVENTS_TO_EMAIL = 2

# Max leads to email per run — keeps it personal, not a blast
# Top N by behavioral urgency priority score
MAX_PER_RUN = 10

# Ylopo tags that indicate behavioral intent (scored separately from LeadStream)
YLOPO_INTENT_TAGS = {
    "Y_HOME_3_VIEW":          "viewed a specific home 3+ times",
    "Y_SHARED_LISTING":       "shared a listing (likely with a partner/spouse)",
    "Y_SELLER_REPORT_VIEWED": "viewed a seller home value report",
    "Y_ADDRESS_FOUND":        "Ylopo identified their current home address",
    "Y_REMARKETING_ENGAGED":  "re-engaged via remarketing ads",
    "AI_NEEDS_FOLLOW_UP":     "Ylopo AI flagged for immediate follow-up",
    "AI_VOICE_NEEDS_FOLLOW_UP": "Ylopo AI voice call needs human follow-up",
    "HANDRAISER":             "raised their hand — expressed direct interest",
    "YPRIORITY":              "Ylopo top-priority buyer signal",
    "HVB":                    "high-value buyer",
    "RETURNED":               "returned to site after a gap",
}

SELL_BEFORE_BUY_TAGS = {
    "I_NEED_TO_SELL_BEFORE_I_CAN_BUY",
    "sell_before_buy=Yes",
}

LOGO_URL = "https://i.postimg.cc/wMttBBmb/legacy-logo.png"
PHYSICAL_ADDRESS = "Legacy Home Team · Virginia Beach, VA"
FROM_EMAIL = "barry@yourfriendlyagent.net"
FROM_NAME  = "Barry Jenkins | Legacy Home Team"


# ---------------------------------------------------------------------------
# Behavior Analyzer
# ---------------------------------------------------------------------------

def _safe_int(val, default=0):
    try:
        return int(float(val)) if val else default
    except (TypeError, ValueError):
        return default


def analyze_behavior(events, tags):
    """
    Parse FUB event data into a structured behavioral profile.

    Returns a dict with:
        views           — list of viewed property dicts (deduped by address)
        saves           — list of saved property dicts
        view_count      — total raw view events
        save_count      — total raw save events
        most_viewed     — property viewed most times (or None)
        most_viewed_ct  — how many times they viewed it
        price_min/max   — price range across all viewed homes
        price_drift     — positive if moving up, negative if moving down (dollars)
        beds_seen       — set of bedroom counts they viewed
        zips            — set of zip codes browsed
        cities          — set of cities browsed
        last_active_dt  — most recent event datetime
        hours_since_last — hours since last IDX event
        session_count   — estimated number of separate browsing sessions
        recent_session  — True if active within 48h
        sell_before_buy — True if they need to sell first
        intent_signals  — list of human-readable intent descriptions from tags
        search_filters  — parsed from Viewed Page URL (price, beds, location)
        registration_prop — property they registered on (if any)
    """
    prop_views = {}   # address → {prop dict, count}
    prop_saves = {}   # address → {prop dict}
    all_prices = []
    time_ordered = []  # (datetime, price) for drift calc
    registration_prop = None

    for e in events:
        e_type = e.get("type", "")
        prop   = e.get("property") or {}
        occurred = e.get("occurred") or e.get("created", "")

        if e_type == "Registration" and prop.get("street"):
            registration_prop = prop

        if e_type in ("Viewed Property", "Property Saved") and prop.get("street"):
            addr  = f"{prop['street']}, {prop.get('city','')} {prop.get('code','')}"
            price = _safe_int(prop.get("price"))

            if e_type == "Viewed Property":
                if addr not in prop_views:
                    prop_views[addr] = {"prop": prop, "count": 0}
                prop_views[addr]["count"] += 1
                if price:
                    all_prices.append(price)
                    try:
                        dt = datetime.fromisoformat(occurred.replace("Z", "+00:00"))
                        time_ordered.append((dt, price))
                    except Exception:
                        pass

            elif e_type == "Property Saved":
                prop_saves[addr] = prop

    # Most-viewed property
    most_viewed = None
    most_viewed_ct = 0
    if prop_views:
        best_addr = max(prop_views, key=lambda a: prop_views[a]["count"])
        most_viewed = prop_views[best_addr]["prop"]
        most_viewed_ct = prop_views[best_addr]["count"]

    # Price range
    price_min = min(all_prices) if all_prices else None
    price_max = max(all_prices) if all_prices else None

    # Price drift — compare avg of earliest half vs latest half of views
    price_drift = 0
    if len(time_ordered) >= 4:
        sorted_by_time = sorted(time_ordered, key=lambda x: x[0])
        half = len(sorted_by_time) // 2
        early_avg = sum(p for _, p in sorted_by_time[:half]) / half
        late_avg  = sum(p for _, p in sorted_by_time[half:]) / (len(sorted_by_time) - half)
        price_drift = int(late_avg - early_avg)

    # Geographic
    zips   = set(v["prop"].get("code","") for v in prop_views.values() if v["prop"].get("code"))
    cities = set(v["prop"].get("city","") for v in prop_views.values() if v["prop"].get("city"))
    beds_seen = set(
        _safe_int(v["prop"].get("bedrooms"))
        for v in prop_views.values() if v["prop"].get("bedrooms")
    )

    # Recency
    all_times = []
    for e in events:
        occ = e.get("occurred") or e.get("created", "")
        try:
            all_times.append(datetime.fromisoformat(occ.replace("Z", "+00:00")))
        except Exception:
            pass
    last_active_dt = max(all_times) if all_times else None
    now_utc = datetime.now(timezone.utc)
    hours_since = (
        (now_utc - last_active_dt).total_seconds() / 3600
        if last_active_dt else None
    )

    # Session estimation: gap > 3h = new session
    session_count = 0
    if all_times:
        sorted_times = sorted(all_times)
        session_count = 1
        for i in range(1, len(sorted_times)):
            gap = (sorted_times[i] - sorted_times[i-1]).total_seconds() / 3600
            if gap > 3:
                session_count += 1

    # Search filters from Viewed Page URLs
    search_filters = _parse_search_urls(events)

    # Intent signals from Ylopo tags
    intent_signals = [
        YLOPO_INTENT_TAGS[t] for t in tags if t in YLOPO_INTENT_TAGS
    ]

    # Sell-before-buy flag
    sell_before_buy = bool(SELL_BEFORE_BUY_TAGS & set(tags))

    return {
        "views":            [v["prop"] for v in prop_views.values()],
        "saves":            list(prop_saves.values()),
        "view_count":       sum(v["count"] for v in prop_views.values()),
        "save_count":       len(prop_saves),
        "most_viewed":      most_viewed,
        "most_viewed_ct":   most_viewed_ct,
        "price_min":        price_min,
        "price_max":        price_max,
        "price_drift":      price_drift,
        "beds_seen":        sorted(beds_seen),
        "zips":             sorted(zips),
        "cities":           sorted(cities),
        "last_active_dt":   last_active_dt,
        "hours_since_last": hours_since,
        "session_count":    session_count,
        "recent_session":   bool(hours_since is not None and hours_since <= 48),
        "sell_before_buy":  sell_before_buy,
        "intent_signals":   intent_signals,
        "search_filters":   search_filters,
        "registration_prop": registration_prop,
    }


def _parse_search_urls(events):
    """Extract search filter params from Viewed Page URL events."""
    filters = {}
    for e in events:
        if e.get("type") != "Viewed Page":
            continue
        url = e.get("pageUrl") or ""
        if "legacyhomesearch" not in url:
            continue
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            # Price range
            if "s[maxPrice]" in qs:
                filters["max_price"] = _safe_int(qs["s[maxPrice]"][0])
            if "s[minPrice]" in qs:
                filters["min_price"] = _safe_int(qs["s[minPrice]"][0])
            # Beds / baths
            if "s[beds]" in qs:
                filters["min_beds"] = _safe_int(qs["s[beds]"][0])
            if "s[baths]" in qs:
                filters["min_baths"] = _safe_int(qs["s[baths]"][0])
            # Location
            for key, vals in qs.items():
                if "[city]" in key:
                    filters.setdefault("cities", []).append(vals[0])
                if "[zip]" in key:
                    filters.setdefault("zips", []).append(vals[0])
        except Exception:
            continue
    return filters


# ---------------------------------------------------------------------------
# Email Strategy Selector
# ---------------------------------------------------------------------------

def select_strategy(behavior, leadstream_tier, tags):
    """
    Choose the right email approach based on behavioral signals.
    Returns (strategy_name, priority) — higher priority = more urgent.
    """
    b = behavior

    # Highest priority: saved a property
    if b["save_count"] > 0:
        return "saved_property", 100

    # Came back to the same property 2+ times
    if b["most_viewed_ct"] >= 2:
        return "repeat_view", 90

    # Active session within 48h with 4+ properties viewed
    if b["recent_session"] and b["view_count"] >= 4:
        return "active_session", 80

    # Sell-before-buy with any activity
    if b["sell_before_buy"] and b["view_count"] >= 2:
        return "seller_buyer", 75

    # Ylopo high-intent tag with any activity
    if leadstream_tier in ("AI_NEEDS_FOLLOW_UP", "HANDRAISER", "YPRIORITY"):
        return "signal_followup", 70

    # Price drift (moved up more than $25K) — searching for more
    if b["price_drift"] > 25000 and b["view_count"] >= 4:
        return "price_drift_up", 60

    # Price drift down — might be tightening budget
    if b["price_drift"] < -25000 and b["view_count"] >= 4:
        return "price_drift_down", 55

    # Has some views but nothing else notable
    if b["view_count"] >= 3:
        return "general_activity", 40

    # Registered but minimal activity
    if b["registration_prop"]:
        return "registration_followup", 30

    return "none", 0


# ---------------------------------------------------------------------------
# Claude Email Generator
# ---------------------------------------------------------------------------

def generate_email(person, behavior, strategy, leadstream_tier, dry_run=False):
    """
    Generate a personalized email using Claude.

    Returns {subject, body_text, body_html} or raises on failure.
    """
    first_name = person.get("firstName") or "there"
    tags = person.get("tags", [])

    # Build the behavioral brief for Claude
    brief = _build_behavioral_brief(first_name, behavior, strategy, leadstream_tier, tags)

    if dry_run:
        logger.info("[DRY RUN] Would call Claude for %s (strategy: %s)", first_name, strategy)
        return {
            "subject": f"[DRY RUN] {strategy} email for {first_name}",
            "body_text": f"[DRY RUN]\n\n{brief[:300]}...",
            "body_html": f"<p>[DRY RUN]</p><pre>{brief[:300]}</pre>",
        }

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — add it to Railway environment variables")

    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed — run: pip install anthropic")

    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""You are writing a short, personal real estate follow-up email for Barry Jenkins,
team leader of Legacy Home Team in Virginia Beach/Hampton Roads, VA.

VOICE GUIDE (mandatory):
- Conversational, like a smart friend who happens to know real estate
- Teaching, never pushing — give insight before asking for anything
- Never shame, never pressure, never use "just checking in"
- Story-first: open with what you noticed, not with a pitch
- Short: under 130 words in the body (not counting subject/PS)
- One clear, low-friction ask at the end
- Always include a P.S. with a second curiosity hook
- Tone: warm, direct, confident — not salesy

LEAD BEHAVIORAL BRIEF:
{brief}

OUTPUT FORMAT (JSON only, no markdown):
{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "The full email body text. Use real line breaks. First line addresses them by first name. No sign-off — just the body and P.S."
}}

Pick the subject that feels most personal and specific to their actual behavior.
Do not use generic real estate phrases like "dream home", "perfect fit", "hot market".
Reference specific property addresses, prices, or neighborhoods when available.
The P.S. should reference something specific that creates curiosity — make them want to reply."""

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Parse JSON response
    import json, re
    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

    data = json.loads(raw)
    subject_options = data.get("subject_options", [])
    # Pick first subject (can A/B test later)
    subject = subject_options[0] if subject_options else f"Following up — {first_name}"
    body_text = data.get("body", "")

    # Add sign-off
    body_text = body_text + "\n\n— Barry\nLegacy Home Team"

    # Render HTML
    body_html = _render_html(body_text)

    return {
        "subject":     subject,
        "body_text":   body_text,
        "body_html":   body_html,
        "all_subjects": subject_options,
    }


def _build_behavioral_brief(first_name, behavior, strategy, leadstream_tier, tags):
    """Build a clear behavioral summary to feed Claude."""
    b = behavior
    lines = [f"LEAD: {first_name}"]
    lines.append(f"LEADSTREAM TIER: {leadstream_tier}")
    lines.append(f"EMAIL STRATEGY: {strategy}")

    # Properties viewed
    if b["views"]:
        lines.append(f"\nPROPERTIES VIEWED ({b['view_count']} views across {len(b['views'])} unique homes):")
        for p in b["views"][:6]:  # cap at 6 to keep prompt tight
            addr  = f"{p.get('street')}, {p.get('city')} {p.get('code')}"
            price = f"${_safe_int(p.get('price')):,}"
            specs = f"{p.get('bedrooms','?')}bd/{p.get('bathrooms','?')}ba, {p.get('area','?')}sf"
            lines.append(f"  - {addr} | {price} | {specs}")

        if b["most_viewed_ct"] >= 2 and b["most_viewed"]:
            p = b["most_viewed"]
            lines.append(f"\nNOTE: They viewed {p.get('street')} {b['most_viewed_ct']} times — unusually high interest.")

    # Saved properties
    if b["saves"]:
        lines.append(f"\nSAVED (highest intent — {len(b['saves'])} saved):")
        for p in b["saves"][:3]:
            addr  = f"{p.get('street')}, {p.get('city')} {p.get('code')}"
            price = f"${_safe_int(p.get('price')):,}"
            specs = f"{p.get('bedrooms','?')}bd/{p.get('bathrooms','?')}ba"
            lines.append(f"  - {addr} | {price} | {specs}")

    # Price context
    if b["price_min"] and b["price_max"]:
        price_range = f"${b['price_min']:,}–${b['price_max']:,}"
        lines.append(f"\nPRICE RANGE BROWSED: {price_range}")
    if abs(b["price_drift"]) > 15000:
        direction = "UP" if b["price_drift"] > 0 else "DOWN"
        lines.append(f"PRICE DRIFT: moved {direction} ${abs(b['price_drift']):,} over their search history")

    # Geography
    if b["cities"]:
        lines.append(f"CITIES: {', '.join(b['cities'])}")
    if b["zips"]:
        lines.append(f"ZIP CODES: {', '.join(b['zips'])}")

    # Recency
    if b["hours_since_last"] is not None:
        hrs = b["hours_since_last"]
        if hrs < 2:
            recency = "ACTIVE RIGHT NOW (within 2 hours)"
        elif hrs < 24:
            recency = f"active {int(hrs)} hours ago"
        elif hrs < 48:
            recency = "active yesterday"
        else:
            recency = f"last active {int(hrs/24)} days ago"
        lines.append(f"RECENCY: {recency}")
    lines.append(f"SESSIONS: {b['session_count']} separate browsing sessions")

    # Seller-buyer situation
    if b["sell_before_buy"]:
        lines.append("\nIMPORTANT: This lead needs to sell their current home before buying. "
                     "Acknowledge this — it's actually workable and they may not know that.")

    # Registration property
    if b["registration_prop"]:
        p = b["registration_prop"]
        addr  = f"{p.get('street')}, {p.get('city')}"
        price = f"${_safe_int(p.get('price')):,}"
        lines.append(f"\nREGISTERED ON: {addr} ({price}) — this was the original hook property.")

    # Intent signals
    if b["intent_signals"]:
        lines.append(f"\nYLOPO SIGNALS: {'; '.join(b['intent_signals'])}")

    return "\n".join(lines)


def _render_html(body_text):
    """Convert plain text email body to clean HTML for SendGrid."""
    paragraphs = body_text.strip().split("\n\n")
    html_parts = []
    for para in paragraphs:
        # P.S. gets a distinct style
        if para.strip().startswith("P.S"):
            html_parts.append(
                f'<p style="font-size:14px;color:#4a5568;border-top:1px solid #e2e8f0;'
                f'padding-top:12px;margin-top:16px">{para.replace(chr(10), "<br>")}</p>'
            )
        else:
            html_parts.append(
                f'<p style="font-size:16px;line-height:1.75;color:#2d3748;margin:0 0 14px">'
                f'{para.replace(chr(10), "<br>")}</p>'
            )

    body_html_inner = "\n".join(html_parts)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">
<div style="max-width:520px;margin:24px auto;background:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
  <div style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:20px 32px;text-align:center">
    <img src="{LOGO_URL}" alt="Legacy Home Team" width="110" style="display:block;margin:0 auto;height:auto">
  </div>
  <div style="padding:28px 32px 20px">
    {body_html_inner}
  </div>
  <div style="background:#f7fafc;padding:14px 32px;text-align:center;border-top:1px solid #e2e8f0">
    <p style="margin:0;font-size:11px;color:#a0aec0;line-height:1.6">
      {PHYSICAL_ADDRESS}<br>
      <a href="https://legacyhometeam.com" style="color:#a0aec0">legacyhometeam.com</a>
      &nbsp;&middot;&nbsp;
      <a href="mailto:{FROM_EMAIL}?subject=Unsubscribe" style="color:#a0aec0">Unsubscribe</a>
    </p>
  </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

def send_email(to_email, subject, body_text, body_html, dry_run=False):
    """Send via SendGrid. Returns {status, sg_message_id}."""
    if dry_run:
        logger.info("[DRY RUN] Would send to %s | %s", to_email, subject)
        return {"status": "dry_run", "sg_message_id": None}

    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY not set")

    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email as SgEmail

    msg = Mail(
        from_email=SgEmail(FROM_EMAIL, FROM_NAME),
        to_emails=to_email,
        subject=subject,
        plain_text_content=body_text,
        html_content=body_html,
    )
    # Set reply-to so replies come back to Barry
    msg.reply_to = SgEmail(FROM_EMAIL, FROM_NAME)

    sg = SendGridAPIClient(api_key)
    resp = sg.send(msg)
    sg_id = resp.headers.get("X-Message-Id") if resp.headers else None
    return {"status": "sent", "code": resp.status_code, "sg_message_id": sg_id}


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pond_mailer(dry_run=True, person_id=None, limit=None):
    """
    Main entry point. Processes LeadStream pond leads and sends personalized emails.

    1. Pull tagged pond leads from FUB
    2. Skip leads emailed within cooldown period
    3. Pull their IDX events
    4. Analyze behavior
    5. Select email strategy
    6. Generate email with Claude
    7. Send via SendGrid
    8. Log to DB
    """
    # Load .env for local dev
    _load_env()

    import db as _db
    from fub_client import FUBClient
    from config import LEADSTREAM_ALLOWED_POND_IDS, LEADSTREAM_POND_TAG

    _db.ensure_pond_email_log_table()

    client = FUBClient()
    now = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Pond Mailer — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # Get pond leads that have the LeadStream_Pond tag
    all_pond_leads = []
    for pond_id in sorted(LEADSTREAM_ALLOWED_POND_IDS):
        leads = client.get_people(pond_id=pond_id)
        for lead in leads:
            if LEADSTREAM_POND_TAG in (lead.get("tags") or []):
                all_pond_leads.append(lead)

    # Filter to single person if specified
    if person_id:
        all_pond_leads = [p for p in all_pond_leads if p.get("id") == person_id]
        if not all_pond_leads:
            # Try fetching directly
            person = client.get_person(person_id)
            if person:
                all_pond_leads = [person]

    logger.info("Found %d tagged pond leads to evaluate", len(all_pond_leads))

    sent = 0
    skipped_cooldown = 0
    skipped_no_email = 0
    skipped_no_activity = 0
    skipped_no_strategy = 0
    max_to_process = limit or MAX_PER_RUN

    for person in all_pond_leads:
        if sent >= max_to_process:
            logger.info("Reached max per run (%d)", max_to_process)
            break

        pid   = person.get("id")
        first = person.get("firstName") or ""
        last  = person.get("lastName") or ""
        name  = f"{first} {last}".strip() or f"ID:{pid}"
        tags  = person.get("tags") or []

        # Get email address
        emails = person.get("emails") or []
        to_email = next(
            (e["value"] for e in emails if e.get("isPrimary") or e.get("status") == "Valid"),
            None
        )
        if not to_email:
            to_email = emails[0]["value"] if emails else None

        if not to_email:
            logger.debug("Skipping %s — no email address", name)
            skipped_no_email += 1
            continue

        # Opt-out check — lead replied negatively to a previous email
        if "PondMailer_Unsubscribed" in tags:
            logger.debug("Skipping %s — opted out", name)
            skipped_cooldown += 1
            continue

        # Cooldown check
        days_ago = _db.days_since_last_pond_email(pid)
        if days_ago is not None and days_ago < EMAIL_COOLDOWN_DAYS:
            logger.debug("Skipping %s — emailed %.1f days ago", name, days_ago)
            skipped_cooldown += 1
            continue

        # Pull IDX events
        events = client.get_events_for_person(pid, days=30)
        property_events = [
            e for e in events
            if e.get("type") in ("Viewed Property", "Property Saved", "Registration")
            and e.get("property")
        ]

        if len(property_events) < MIN_EVENTS_TO_EMAIL:
            logger.debug("Skipping %s — only %d property events", name, len(property_events))
            skipped_no_activity += 1
            continue

        # Analyze behavior
        behavior = analyze_behavior(events, tags)

        # Determine LeadStream tier from tags
        leadstream_tier = "POND"
        for tier_tag in ("AI_NEEDS_FOLLOW_UP", "HANDRAISER", "YPRIORITY",
                          "AI_VOICE_NEEDS_FOLLOW_UP", "Y_HOME_3_VIEW"):
            if tier_tag in tags:
                leadstream_tier = tier_tag
                break

        # Select strategy
        strategy, priority = select_strategy(behavior, leadstream_tier, tags)

        if strategy == "none" or priority < 30:
            logger.debug("Skipping %s — no compelling email strategy (score %d)", name, priority)
            skipped_no_strategy += 1
            continue

        # Log what we found
        print(f"\n  [{strategy.upper()}] {name} (ID: {pid})")
        print(f"    Email: {to_email}")
        print(f"    Tier: {leadstream_tier} | Views: {behavior['view_count']} | Saves: {behavior['save_count']}")
        if behavior["most_viewed"]:
            p = behavior["most_viewed"]
            print(f"    Most viewed: {p.get('street')} ({behavior['most_viewed_ct']}x)")
        if behavior["sell_before_buy"]:
            print(f"    ⚠ Sell-before-buy")
        if behavior["hours_since_last"]:
            hrs = behavior["hours_since_last"]
            print(f"    Last active: {int(hrs)}h ago" if hrs < 48 else f"    Last active: {int(hrs/24)}d ago")

        # Generate email with Claude
        try:
            email_data = generate_email(person, behavior, strategy, leadstream_tier, dry_run=dry_run)
        except Exception as e:
            logger.error("Claude generation failed for %s: %s", name, e)
            continue

        print(f"    Subject: {email_data['subject']}")
        if dry_run:
            print(f"\n    --- PREVIEW ---")
            print("    " + email_data["body_text"].replace("\n", "\n    ")[:400])
            print(f"    --- END PREVIEW ---\n")

        # Send
        try:
            result = send_email(
                to_email=to_email,
                subject=email_data["subject"],
                body_text=email_data["body_text"],
                body_html=email_data["body_html"],
                dry_run=dry_run,
            )
        except Exception as e:
            logger.error("Send failed for %s: %s", name, e)
            continue

        # Log
        behavior_summary = (
            f"Views:{behavior['view_count']} Saves:{behavior['save_count']} "
            f"Price:${behavior.get('price_min',0):,}-${behavior.get('price_max',0):,} "
            f"Cities:{','.join(behavior['cities'])}"
        )
        _db.log_pond_email(
            person_id=pid,
            person_name=name,
            email_address=to_email,
            subject=email_data["subject"],
            strategy=strategy,
            leadstream_tier=leadstream_tier,
            behavior_summary=behavior_summary,
            dry_run=dry_run,
            sg_message_id=result.get("sg_message_id"),
        )

        sent += 1
        print(f"    ✓ {'[DRY RUN] Would send' if dry_run else 'Sent'}")

    print(f"\n{'='*60}")
    print(f"  Done: {sent} {'would send' if dry_run else 'sent'} | "
          f"Cooldown: {skipped_cooldown} | No activity: {skipped_no_activity} | "
          f"No email: {skipped_no_email} | No strategy: {skipped_no_strategy}")
    print(f"{'='*60}\n")

    return {
        "sent":                 sent,
        "skipped_cooldown":     skipped_cooldown,
        "skipped_no_email":     skipped_no_email,
        "skipped_no_activity":  skipped_no_activity,
        "skipped_no_strategy":  skipped_no_strategy,
        "dry_run":              dry_run,
    }


def _load_env():
    """Load .env for local dev. No-op if already set or file missing."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pond Mailer: LeadStream email marketing")
    parser.add_argument("--apply",  action="store_true", help="Send live emails (default: dry run)")
    parser.add_argument("--person", type=int, default=None, help="FUB person ID for single-lead test")
    parser.add_argument("--limit",  type=int, default=None, help="Max leads to process")
    args = parser.parse_args()

    run_pond_mailer(
        dry_run=not args.apply,
        person_id=args.person,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
