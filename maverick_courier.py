"""
maverick_courier.py — nightly Maverick call-log courier for the always-on Mac.

MaverickRE has no API and only emails coach reports to the assigned agent.
This script bridges that: a persistent Chromium profile holds Barry's login
(entered ONCE by Barry, never stored in this script), and every night the
courier opens the call-log page headlessly, extracts everything visible,
and posts it to Command Center's ingest endpoint, where agents and grades
are matched against the roster.

SETUP (one time, on the always-on Mac, in Terminal):
    cd ~/fub-kpi-audit && git pull
    pip3 install playwright requests && python3 -m playwright install chromium
    python3 maverick_courier.py --setup
        -> a browser window opens. Log into maverickre.com, click through to
           the CALL LOG page you want harvested, then return to Terminal and
           press Enter. The URL and your session are saved locally.
    python3 maverick_courier.py --run    (test one harvest immediately)
Then schedule it nightly (9:05pm):
    crontab -e   ->   5 21 * * * cd ~/fub-kpi-audit && /usr/bin/python3 maverick_courier.py --run >> ~/maverick_courier.log 2>&1

If Maverick logs the session out, the nightly run posts a LOGIN_NEEDED
alert to Command Center (it shows amber in Mission Control) and Barry just
reruns --setup. Credentials are never typed by software or stored anywhere;
only the browser profile's own cookies persist, on this machine.
"""

import argparse
import json
import os
import sys
from datetime import datetime

import requests

CONFIG_PATH = os.path.expanduser("~/.maverick_courier.json")
PROFILE_DIR = os.path.expanduser("~/.maverick_courier_profile")
INGEST_URL = "https://www.legacycommandcenter.com/api/admin/maverick/ingest"
FALLBACK_INGEST = "https://web-production-3363cc.up.railway.app/api/admin/maverick/ingest"
KEY = "lht-perp-2026"
MAX_CHARS = 18000  # ingest stores 20k; leave headroom


def _load_config():
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    os.chmod(CONFIG_PATH, 0o600)


def _post(raw, source):
    body = {"raw": raw, "source": source}
    for url in (INGEST_URL, FALLBACK_INGEST):
        try:
            r = requests.post(url, params={"key": KEY}, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            continue
    return None


def setup():
    from playwright.sync_api import sync_playwright
    print("Opening browser. Log into maverickre.com, navigate to the CALL LOG")
    print("page you want harvested nightly, then come back here and press Enter.")
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(PROFILE_DIR, headless=False)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto("https://maverickre.com")
        input("\n>>> Press Enter here once the call log page is on screen... ")
        url = page.url
        _save_config({"call_log_url": url, "setup_at": datetime.now().isoformat()})
        print(f"Saved. Nightly harvest will open: {url}")
        ctx.close()


def _looks_logged_out(final_url, text):
    lowered = (text or "").lower()
    url = (final_url or "").lower()
    return ("login" in url or "sign-in" in url or "signin" in url
            or ("password" in lowered and "log in" in lowered)
            or len((text or "").strip()) < 200)


def run():
    cfg = _load_config()
    url = cfg.get("call_log_url")
    if not url:
        print("No config. Run: python3 maverick_courier.py --setup")
        sys.exit(1)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(PROFILE_DIR, headless=True)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(4000)  # let client-side tables render

        # Generic extraction: structured rows first, full text as safety net.
        chunks = []
        try:
            rows = page.eval_on_selector_all(
                "table tr, [role='row']",
                "els => els.map(e => e.innerText.replace(/\\n/g, ' | '))")
            if rows and len(rows) > 2:
                chunks.append("MAVERICK CALL LOG ROWS\n" + "\n".join(rows))
        except Exception:
            pass
        try:
            body_text = page.inner_text("body")
        except Exception:
            body_text = ""
        final_url = page.url
        ctx.close()

    if _looks_logged_out(final_url, body_text):
        _post("LOGIN_NEEDED: Maverick courier session expired on the always-on "
              "Mac. Rerun: python3 maverick_courier.py --setup", "courier")
        print("LOGIN_NEEDED posted. Rerun --setup on this Mac.")
        sys.exit(2)

    if not chunks:
        chunks.append("MAVERICK PAGE TEXT\n" + body_text)
    stamp = f"[courier {datetime.now().isoformat()[:16]}] "
    ok = 0
    for chunk in chunks:
        payload = stamp + chunk
        for i in range(0, len(payload), MAX_CHARS):
            res = _post(payload[i:i + MAX_CHARS], "courier")
            if res and res.get("ok"):
                ok += 1
    print(f"posted {ok} chunk(s) at {datetime.now().isoformat()[:19]}")
    sys.exit(0 if ok else 3)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", action="store_true", help="one-time login + page pick")
    ap.add_argument("--run", action="store_true", help="harvest and post")
    args = ap.parse_args()
    if args.setup:
        setup()
    elif args.run:
        run()
    else:
        ap.print_help()
