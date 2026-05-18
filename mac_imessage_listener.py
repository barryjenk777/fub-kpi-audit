#!/usr/bin/env python3
"""
Mac iMessage Listener — Barry's MacBook Pro
============================================
Runs as a local Flask server. Cloudflare Tunnel exposes it publicly so
Railway can POST coaching texts and have them sent from Barry's personal
iMessage account via AppleScript.

SETUP (run once on the Mac):
    pip3 install flask requests
    brew install cloudflared

RUN (two terminals):
    Terminal 1:  python3 mac_imessage_listener.py
    Terminal 2:  cloudflared tunnel --url http://localhost:8765

Copy the Cloudflare URL (e.g. https://xxx.trycloudflare.com) and set it
in Railway environment variables:
    MAC_IMESSAGE_URL=https://xxx.trycloudflare.com
    MAC_IMESSAGE_SECRET=lht-mac-2026

POLLER MODE (optional backup):
    python3 mac_imessage_listener.py --poll
Polls Railway every 60 seconds for pending texts and sends them,
even if the webhook URL changes. Good fallback if Cloudflare URL rotates.
"""

import argparse
import subprocess
import time
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mac_listener")

RAILWAY_API    = "https://web-production-3363cc.up.railway.app"
RAILWAY_KEY    = "lht-perp-2026"
LOCAL_SECRET   = os.environ.get("MAC_IMESSAGE_SECRET", "lht-mac-2026")
PORT           = int(os.environ.get("PORT", 8765))


# ---------------------------------------------------------------------------
# AppleScript sender
# ---------------------------------------------------------------------------

def send_imessage(phone: str, message: str) -> tuple[bool, str]:
    """
    Send an iMessage via AppleScript. Returns (success, error_or_empty).

    Requires:
      - Messages.app is open and Barry is signed in to iMessage
      - System Preferences > Security > Accessibility grants Terminal (or Python)
      - The recipient has iMessage enabled on their number
    """
    # Escape backslashes and double-quotes for AppleScript string safety
    safe_msg   = message.replace("\\", "\\\\").replace('"', '\\"')
    safe_phone = phone.strip()

    script = f'''
tell application "Messages"
    set targetBuddy to "{safe_phone}"
    set targetService to 1st account whose service type = iMessage
    send "{safe_msg}" to participant targetBuddy of targetService
end tell
'''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            logger.info("Sent iMessage to %s: %r", safe_phone, message[:60])
            return True, ""
        else:
            err = result.stderr.strip() or result.stdout.strip()
            logger.error("AppleScript failed for %s: %s", safe_phone, err)
            return False, err
    except subprocess.TimeoutExpired:
        return False, "AppleScript timed out after 15s"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Webhook listener mode
# ---------------------------------------------------------------------------

def run_webhook_server():
    """Start Flask server to receive webhook POSTs from Railway."""
    from flask import Flask, request, jsonify

    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"ok": True, "mode": "webhook_listener"})

    @app.route("/send", methods=["POST"])
    def send():
        data = request.get_json(force=True, silent=True) or {}

        if data.get("key") != LOCAL_SECRET:
            logger.warning("Unauthorized /send attempt")
            return jsonify({"error": "unauthorized"}), 401

        phone   = (data.get("phone") or "").strip()
        message = (data.get("message") or "").strip()
        queue_id = data.get("queue_id")

        if not phone or not message:
            return jsonify({"error": "phone and message required"}), 400

        ok, err = send_imessage(phone, message)

        # Report back to Railway so it can mark the queue row
        if queue_id:
            try:
                import requests as _req
                if ok:
                    _req.post(
                        f"{RAILWAY_API}/api/admin/agent-texts/mark-sent",
                        json={"ids": [queue_id]},
                        params={"key": RAILWAY_KEY},
                        timeout=8,
                    )
                else:
                    _req.post(
                        f"{RAILWAY_API}/api/admin/agent-texts/mark-failed",
                        json={"id": queue_id, "error": err},
                        params={"key": RAILWAY_KEY},
                        timeout=8,
                    )
            except Exception as _re:
                logger.warning("Could not report status to Railway: %s", _re)

        if ok:
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": err}), 500

    logger.info("Mac iMessage listener starting on port %d", PORT)
    logger.info("Set MAC_IMESSAGE_URL in Railway to your Cloudflare Tunnel URL")
    app.run(host="0.0.0.0", port=PORT, debug=False)


# ---------------------------------------------------------------------------
# Poller mode (backup)
# ---------------------------------------------------------------------------

def run_poller(interval_seconds=60):
    """
    Poll Railway for pending texts and send them via AppleScript.
    Good fallback if the Cloudflare Tunnel URL rotates.
    """
    import requests as _req

    logger.info("Mac iMessage poller starting — checking every %ds", interval_seconds)

    while True:
        try:
            resp = _req.get(
                f"{RAILWAY_API}/api/admin/agent-texts/pending",
                params={"key": RAILWAY_KEY},
                timeout=10,
            )
            if not resp.ok:
                logger.warning("Railway returned %d", resp.status_code)
            else:
                data    = resp.json()
                pending = data.get("pending", [])
                if pending:
                    logger.info("Found %d pending texts", len(pending))

                sent_ids   = []
                failed_ids = []

                for item in pending:
                    phone   = item.get("phone", "")
                    message = item.get("message", "")
                    row_id  = item.get("id")
                    agent   = item.get("agent_name", "?")

                    ok, err = send_imessage(phone, message)
                    if ok:
                        sent_ids.append(row_id)
                    else:
                        logger.error("Failed to send to %s (%s): %s", agent, phone, err)
                        failed_ids.append((row_id, err))

                if sent_ids:
                    _req.post(
                        f"{RAILWAY_API}/api/admin/agent-texts/mark-sent",
                        json={"ids": sent_ids},
                        params={"key": RAILWAY_KEY},
                        timeout=8,
                    )
                    logger.info("Marked %d texts as sent", len(sent_ids))

                for row_id, err in failed_ids:
                    _req.post(
                        f"{RAILWAY_API}/api/admin/agent-texts/mark-failed",
                        json={"id": row_id, "error": err},
                        params={"key": RAILWAY_KEY},
                        timeout=8,
                    )

        except Exception as e:
            logger.error("Poller error: %s", e)

        time.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Barry's Mac iMessage sender")
    parser.add_argument("--poll", action="store_true",
                        help="Run in poller mode instead of webhook listener")
    parser.add_argument("--interval", type=int, default=60,
                        help="Poll interval in seconds (default: 60)")
    parser.add_argument("--test", metavar="PHONE",
                        help="Send a test message to this phone number and exit")
    args = parser.parse_args()

    if args.test:
        ok, err = send_imessage(args.test, "Test from Legacy Home Team Mac listener. If you got this, it works.")
        print("OK" if ok else f"FAILED: {err}")
    elif args.poll:
        run_poller(interval_seconds=args.interval)
    else:
        run_webhook_server()
