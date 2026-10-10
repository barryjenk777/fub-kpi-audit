"""
slack_client.py — minimal Slack Web API client for the Lead Desk.

No SDK: a few endpoints over requests, same pattern as fub_client. Every
function is a silent no-op (returns None/False) when SLACK_BOT_TOKEN is
unset, so callers can fire-and-forget without config checks and the app
deploys safely before the Slack app exists.

Env (Railway, staged-then-Deploy):
  SLACK_BOT_TOKEN         xoxb-... bot token. Scopes: chat:write, im:write,
                          users:read, users:read.email
  SLACK_SIGNING_SECRET    verifies X-Slack-Signature on /api/slack/actions
  SLACK_DESK_OPS_CHANNEL  channel id (C...) for Phase 1 mirror cards
"""

import hashlib
import hmac
import logging
import os
import time

import requests

logger = logging.getLogger("slack")
_API = "https://slack.com/api"


def is_available():
    return bool(os.environ.get("SLACK_BOT_TOKEN"))


def _auth_header():
    return {"Authorization": "Bearer %s" % os.environ.get("SLACK_BOT_TOKEN", "")}


def post_message(channel, text, blocks=None):
    """chat.postMessage. Returns the message ts on success, None otherwise.
    `channel` may be a channel id (C...) or a user id (U...) — posting to a
    user id opens/uses the bot DM, which is how desk offers travel."""
    if not is_available() or not channel:
        return None
    try:
        payload = {"channel": channel, "text": text}
        if blocks:
            payload["blocks"] = blocks
        r = requests.post(f"{_API}/chat.postMessage",
                          headers={**_auth_header(),
                                   "Content-Type": "application/json; charset=utf-8"},
                          json=payload, timeout=15).json()
        if not r.get("ok"):
            logger.warning("slack post failed (%s): %s", channel, r.get("error"))
            return None
        return r.get("ts")
    except Exception as e:
        logger.warning("slack post error: %s", e)
        return None


def dm_user(user_id, text, blocks=None):
    """DM a user by Slack id."""
    return post_message(user_id, text, blocks=blocks)


def lookup_user_by_email(email):
    """users.lookupByEmail → Slack user id, or None."""
    if not is_available() or not email:
        return None
    try:
        r = requests.get(f"{_API}/users.lookupByEmail", headers=_auth_header(),
                         params={"email": email}, timeout=15).json()
        if r.get("ok"):
            return (r.get("user") or {}).get("id")
        logger.info("slack email lookup miss for %s: %s", email, r.get("error"))
    except Exception as e:
        logger.warning("slack lookup error: %s", e)
    return None


def signature_ok(headers, raw_body):
    """Verify Slack's v0 request signature. False when the secret is unset,
    so the actions endpoint refuses everything until configured."""
    secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if not secret:
        return False
    try:
        ts = headers.get("X-Slack-Request-Timestamp", "")
        if abs(time.time() - float(ts)) > 300:
            return False  # replay guard
        base = b"v0:" + ts.encode() + b":" + raw_body
        digest = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
        return hmac.compare_digest(digest, headers.get("X-Slack-Signature", ""))
    except Exception:
        return False


def group_dm(user_ids, text, blocks=None):
    """One message both people see: conversations.open with multiple user
    ids returns the group-DM channel, then post there. Falls back to
    individual DMs if the workspace scope disallows mpim."""
    ids = [u for u in (user_ids or []) if u]
    if not is_available() or not ids:
        return None
    if len(ids) == 1:
        return post_message(ids[0], text, blocks=blocks)
    try:
        r = requests.post(f"{_API}/conversations.open",
                          headers={**_auth_header(),
                                   "Content-Type": "application/json; charset=utf-8"},
                          json={"users": ",".join(ids)}, timeout=15).json()
        if r.get("ok"):
            ch = ((r.get("channel") or {}).get("id"))
            if ch:
                return post_message(ch, text, blocks=blocks)
        logger.warning("group dm open failed: %s — falling back to singles",
                       r.get("error"))
    except Exception as e:
        logger.warning("group dm error: %s — falling back to singles", e)
    ts = None
    for u in ids:
        ts = post_message(u, text, blocks=blocks) or ts
    return ts
