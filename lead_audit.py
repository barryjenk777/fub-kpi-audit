"""
Lead Audit Emailer — sends a full breakdown to Barry after every SMS touch.

Called in two places:
  1. pond_mailer.run_new_lead_mailer()  — after initial text goes out
  2. app.py webhook                     — after voice note or video is sent on consent

Each email is a complete record of who the lead is, what we sent, what they said,
and what we sent back. Barry can read these to QA the scripts before trusting the
system at scale.

All emails go to AUDIT_EMAIL (barry@yourfriendlyagent.net).
"""

import logging
import os

logger = logging.getLogger(__name__)

AUDIT_EMAIL = "barry@yourfriendlyagent.net"
AUDIT_FROM  = "barry@yourfriendlyagent.net"   # SendGrid sender


def _behavior_lines(behavior: dict) -> list[str]:
    """Turn a behavior dict into readable bullet lines."""
    b = behavior or {}
    lines = []

    if b.get("most_viewed") and b.get("most_viewed_ct", 0) >= 1:
        mv = b["most_viewed"]
        addr = ""
        if mv.get("street") and mv.get("city"):
            addr = f"{mv['street']}, {mv['city']}"
        elif mv.get("street"):
            addr = mv["street"]
        elif mv.get("city"):
            addr = mv["city"]
        price = f"  (${int(mv['price']):,})" if mv.get("price") else ""
        if addr:
            lines.append(f"  Most viewed: {addr}{price}  x{b['most_viewed_ct']}")

    if b.get("saves"):
        for sp in b["saves"][:3]:
            sa = ""
            if sp.get("street") and sp.get("city"):
                sa = f"{sp['street']}, {sp['city']}"
            elif sp.get("street"):
                sa = sp["street"]
            sprice = f"  (${int(sp['price']):,})" if sp.get("price") else ""
            if sa:
                lines.append(f"  Saved: {sa}{sprice}")

    if b.get("price_min") and b.get("price_max"):
        line = f"  Price range: ${b['price_min']:,} to ${b['price_max']:,}"
        if b.get("price_drift") and abs(b["price_drift"]) > 10000:
            direction = "up" if b["price_drift"] > 0 else "down"
            line += f"  (drifted {direction} ${abs(b['price_drift']):,})"
        lines.append(line)
    elif b.get("price_max"):
        lines.append(f"  Price max: ${b['price_max']:,}")

    cities = sorted(b.get("cities") or [])
    if cities:
        lines.append(f"  Cities: {', '.join(cities[:4])}")

    beds = sorted(b.get("beds_seen") or [])
    if len(beds) > 1:
        lines.append(f"  Beds: {min(beds)}-{max(beds)} br range")
    elif beds:
        lines.append(f"  Beds: {beds[0]} br")

    vc = b.get("view_count", 0)
    sc = b.get("session_count", 0)
    sv = b.get("save_count", 0)
    if vc:
        lines.append(f"  Activity: {vc} views across {sc} sessions, {sv} saves")

    if b.get("hours_since_last") is not None:
        hrs = b["hours_since_last"]
        if hrs < 1:
            lines.append("  Last active: within the last hour (HOT)")
        elif hrs < 24:
            lines.append(f"  Last active: {int(hrs)}h ago")
        else:
            lines.append(f"  Last active: {int(hrs/24)} days ago")

    if not lines:
        lines.append("  No IDX behavioral data yet (brand new lead)")

    return lines


def send_outreach_audit(
    person_id,
    person_name: str,
    person_source: str,
    lead_type: str,           # "buyer" | "seller" | "zbuyer"
    phone: str,
    sms_body: str,
    ab_variant: str,
    channel: str,
    behavior: dict = None,
    dry_run: bool = False,
):
    """
    Fire after the initial SMS goes out.
    Documents who the lead is, what we know about them, and the exact text sent.
    """
    if dry_run:
        logger.info("[DRY RUN] Audit email suppressed for %s", person_name)
        return

    lead_label = {
        "zbuyer":  "Zbuyer (cash offer request — homeowner)",
        "seller":  "Seller (Ylopo Prospecting — home value inquiry)",
        "buyer":   "Buyer (IDX — actively browsing)",
    }.get(lead_type, lead_type)

    beh_lines = _behavior_lines(behavior)

    body_lines = [
        f"LEAD AUDIT — INITIAL OUTREACH",
        f"{'=' * 52}",
        f"",
        f"LEAD INFO",
        f"  Name:     {person_name}",
        f"  FUB ID:   {person_id}",
        f"  Source:   {person_source or 'unknown'}",
        f"  Type:     {lead_label}",
        f"  Phone:    {phone}",
        f"",
        f"BEHAVIORAL DATA (what we know right now)",
    ] + beh_lines + [
        f"",
        f"OUTREACH SENT",
        f"  Channel:   {channel}",
        f"  A/B variant: {ab_variant}  (voice = ElevenLabs audio bubble on consent)",
        f"                               (video = HeyGen map video on consent)",
        f"",
        f"  TEXT SENT:",
        f"  {'-' * 48}",
    ]

    for line in sms_body.split("\n"):
        body_lines.append(f"  {line}")

    body_lines += [
        f"  {'-' * 48}",
        f"",
        f"WHAT HAPPENS NEXT",
        f"  If they reply 'ok/sure/yes': {ab_variant} recording fires automatically.",
        f"  If positive buying intent: SMS_Conversion tag applied + handoff text in 4.5 min.",
        f"  If they say stop: SMS_OptOut tag applied, no further texts.",
        f"",
        f"{'=' * 52}",
        f"Legacy Home Team AI Outreach",
    ]

    _send(
        subject=f"Audit: Text sent to {person_name} ({lead_label.split('(')[0].strip()})",
        body="\n".join(body_lines),
    )


def send_response_audit(
    person_id,
    person_name: str,
    lead_type: str,           # "buyer" | "seller" | "zbuyer"
    phone: str,
    reply_text: str,
    sentiment: str,
    sentiment_reason: str,
    consent: bool,
    ab_variant: str,
    voice_script: str = None,   # full script if voice note was sent
    video_id: str = None,       # HeyGen video ID if video was sent
    video_url: str = None,
    handoff_delay_seconds: int = None,
    original_sms: str = None,
    behavior: dict = None,
):
    """
    Fire after we process an inbound reply (consent, positive, negative, or neutral).
    Includes the full voice note script so Barry can QA exactly what was said.
    """
    lead_label = {
        "zbuyer":  "Zbuyer (cash offer)",
        "seller":  "Seller (home value)",
        "buyer":   "Buyer (IDX)",
    }.get(lead_type, lead_type)

    sentiment_emoji = {"positive": "POSITIVE", "negative": "NEGATIVE", "neutral": "NEUTRAL"}.get(sentiment, sentiment.upper())

    beh_lines = _behavior_lines(behavior)

    body_lines = [
        f"LEAD AUDIT — INBOUND REPLY",
        f"{'=' * 52}",
        f"",
        f"LEAD INFO",
        f"  Name:   {person_name}",
        f"  FUB ID: {person_id}",
        f"  Type:   {lead_label}",
        f"  Phone:  {phone}",
        f"",
        f"THEIR REPLY",
        f"  Sentiment:  {sentiment_emoji}",
        f"  Reason:     {sentiment_reason}",
        f"  Consent:    {'YES — recording was sent' if consent else 'NO'}",
        f"",
        f"  \"{reply_text.strip()[:600]}\"",
        f"",
    ]

    if original_sms:
        body_lines += [
            f"ORIGINAL TEXT WE SENT",
            f"  \"{original_sms[:400]}\"",
            f"",
        ]

    body_lines += [
        f"BEHAVIORAL DATA",
    ] + beh_lines + [f""]

    if voice_script:
        body_lines += [
            f"VOICE NOTE SENT (ElevenLabs — Barry's cloned voice)",
            f"  {'-' * 48}",
            f"  FULL SCRIPT / TRANSCRIPTION:",
            f"",
        ]
        for line in voice_script.split("\n"):
            body_lines.append(f"  {line}")
        body_lines += [
            f"",
            f"  {'-' * 48}",
            f"  NOTE: This is word-for-word what was spoken in Barry's voice.",
            f"  If the script is wrong, reply to this email and flag it.",
            f"",
        ]

    if video_id:
        body_lines += [
            f"VIDEO SENT (HeyGen)",
            f"  Video ID:  {video_id}",
            f"  Video URL: {video_url or 'see Railway /v/' + video_id}",
            f"",
        ]

    if not voice_script and not video_id and consent:
        body_lines += [
            f"RECORDING STATUS",
            f"  Consent detected but no recording was sent.",
            f"  Check logs — ElevenLabs or Project Blue may have failed.",
            f"",
        ]

    # What happened next
    actions = []
    if sentiment == "positive":
        actions.append("SMS_Conversion + Claude_Text_Converted tags applied in FUB")
        if handoff_delay_seconds:
            actions.append(f"Handoff text queued ({handoff_delay_seconds // 60} min delay)")
    elif sentiment == "negative":
        actions.append("SMS_OptOut tag applied — no further texts")
    elif consent:
        if handoff_delay_seconds:
            actions.append(f"Soft follow-up text queued ({handoff_delay_seconds // 60} min delay — time to listen first)")
    else:
        actions.append("FUB note posted, no further automated action")

    body_lines += [
        f"ACTIONS TAKEN",
    ] + [f"  {a}" for a in actions] + [
        f"",
        f"{'=' * 52}",
        f"Legacy Home Team AI Outreach",
    ]

    consent_tag = " CONSENT + RECORDING" if consent else ""
    _send(
        subject=f"Audit: {person_name} replied — {sentiment_emoji}{consent_tag}",
        body="\n".join(body_lines),
    )


def _send(subject: str, body: str):
    """Send audit email via SendGrid."""
    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail

        sg_key = os.environ.get("SENDGRID_API_KEY")
        if not sg_key:
            logger.warning("Lead audit email skipped — SENDGRID_API_KEY not set")
            return

        html_body = (
            "<div style='font-family:-apple-system,Helvetica,sans-serif;"
            "font-size:14px;line-height:1.7;color:#1a1a1a;"
            "max-width:620px;margin:24px auto;white-space:pre-wrap'>"
            + body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                  .replace("\n", "<br>")
            + "</div>"
        )

        msg = Mail(
            from_email=AUDIT_FROM,
            to_emails=AUDIT_EMAIL,
            subject=f"[Lead Audit] {subject}",
            plain_text_content=body,
            html_content=html_body,
        )
        sendgrid.SendGridAPIClient(sg_key).send(msg)
        logger.info("Lead audit email sent: %s", subject)
    except Exception as e:
        logger.warning("Lead audit email failed: %s", e)
