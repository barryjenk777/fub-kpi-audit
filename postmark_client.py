"""
Postmark email client — used for ALL internal emails:
  nudge_engine.py   (agent daily coaching nudges)
  email_report.py   (KPI audit, hype, ISA, appointment accountability, onboarding)

Outbound lead nurture (pond_mailer.py) stays on SendGrid.

Environment variable required:
  POSTMARK_API_KEY  — Server API Token from postmarkapp.com
                      Settings → Servers → Legacy Internal → API Tokens
"""

import os
import requests as _requests

_POSTMARK_URL = "https://api.postmarkapp.com/email"


def send(
    to: str,
    from_email: str,
    subject: str,
    html: str,
    text: str = "",
    cc=None,
    reply_to: str = "",
) -> dict:
    """
    Send a single email via Postmark.

    Args:
        to         — recipient address or "Name <email>" string
        from_email — verified sender signature address
        subject    — email subject line
        html       — HTML body
        text       — plain-text fallback (optional but recommended)
        cc         — string address, or list of addresses
        reply_to   — reply-to address (optional)

    Returns:
        Postmark API response dict on success.

    Raises:
        RuntimeError  — if POSTMARK_API_KEY is not set
        requests.HTTPError — if Postmark rejects the request
    """
    token = os.environ.get("POSTMARK_API_KEY", "")
    if not token:
        raise RuntimeError(
            "POSTMARK_API_KEY not set. "
            "Get your Server API Token from postmarkapp.com → Servers → Legacy Internal → API Tokens, "
            "then add it to Railway environment variables."
        )

    payload: dict = {
        "From": from_email,
        "To": to,
        "Subject": subject,
        "HtmlBody": html,
        "TextBody": text or _html_to_text(html),
        "MessageStream": "outbound",
        # Postmark defaults: tracking OFF — better for deliverability to iCloud/Apple
        "TrackOpens": False,
        "TrackLinks": "None",
    }

    if cc:
        if isinstance(cc, (list, tuple)):
            payload["Cc"] = ", ".join(str(c) for c in cc)
        else:
            payload["Cc"] = str(cc)

    if reply_to:
        payload["ReplyTo"] = reply_to

    resp = _requests.post(
        _POSTMARK_URL,
        headers={
            "X-Postmark-Server-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=30,
    )

    try:
        resp.raise_for_status()
    except _requests.HTTPError as exc:
        body = resp.text[:500]
        raise _requests.HTTPError(
            f"Postmark rejected send to {to}: {resp.status_code} — {body}",
            response=resp,
        ) from exc

    return resp.json()


def send_batch(messages: list[dict]) -> list[dict]:
    """
    Send up to 500 emails in a single Postmark batch request.

    Each dict in messages should have keys matching send() args:
      to, from_email, subject, html, text (optional), cc (optional)

    Returns list of Postmark response dicts.
    """
    token = os.environ.get("POSTMARK_API_KEY", "")
    if not token:
        raise RuntimeError("POSTMARK_API_KEY not set")

    payload = []
    for m in messages:
        item: dict = {
            "From": m["from_email"],
            "To": m["to"],
            "Subject": m["subject"],
            "HtmlBody": m["html"],
            "TextBody": m.get("text", "") or _html_to_text(m["html"]),
            "MessageStream": "outbound",
            "TrackOpens": False,
            "TrackLinks": "None",
        }
        if m.get("cc"):
            cc = m["cc"]
            item["Cc"] = ", ".join(str(c) for c in cc) if isinstance(cc, (list, tuple)) else str(cc)
        payload.append(item)

    resp = _requests.post(
        "https://api.postmarkapp.com/email/batch",
        headers={
            "X-Postmark-Server-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _html_to_text(html: str) -> str:
    """Very basic HTML → plain text strip for fallback text bodies."""
    import re
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
