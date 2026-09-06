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
        # Maverick's "Log in" opens the app in a NEW TAB, so the tab we
        # started on may still be the marketing site. Pick the best tab:
        # the last one that is neither blank nor the marketing homepage.
        candidates = []
        for p in ctx.pages:
            u = (p.url or "").rstrip("/")
            if u and u != "about:blank" and u not in (
                    "https://maverickre.com", "https://www.maverickre.com"):
                candidates.append(u)
        url = candidates[-1] if candidates else page.url
        print("\nOpen tabs found:")
        for p in ctx.pages:
            marker = "  <-- SAVED" if (p.url or "").rstrip("/") == url.rstrip("/") else ""
            print(f"  {p.url}{marker}")
        _save_config({"call_log_url": url, "setup_at": datetime.now().isoformat()})
        print(f"\nSaved. Nightly harvest will open: {url}")
        print("If that is NOT the sales grading page, rerun --setup and make "
              "sure the grading page is the LAST tab you opened.")
        ctx.close()


def _looks_logged_out(final_url, text):
    lowered = (text or "").lower()
    url = (final_url or "").lower()
    # The marketing homepage means we got bounced out of the app (or the
    # saved URL was wrong) — never post that as data.
    on_marketing = "book a demo" in lowered and "measure what matters" in lowered
    return (on_marketing
            or "login" in url or "sign-in" in url or "signin" in url
            or ("password" in lowered and "log in" in lowered)
            or len((text or "").strip()) < 200)


_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _harvest(url, headless, debug=False):
    """One browser pass. Returns (final_url, body_text, chunks)."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            PROFILE_DIR, headless=headless, user_agent=_UA,
            args=["--disable-blink-features=AutomationControlled"])
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(9000)  # SPAs render slowly; give tables time
        try:
            if len((page.inner_text("body") or "").strip()) < 200:
                page.wait_for_timeout(9000)  # one more chance before judging
        except Exception:
            pass
        if debug:
            txt = ""
            try:
                txt = page.inner_text("body")
            except Exception:
                pass
            print("FINAL URL:", page.url)
            print("TEXT LENGTH:", len(txt or ""))
            print("FIRST 400 CHARS:\n", (txt or "")[:400])
            input("\n(debug) browser is visible — press Enter to close... ")
            ctx.close()
            return page.url, txt, []
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

        # Second harvest: flip Calls Type to "AI Coach" (practice reps).
        # The filter lives in a modal and the URL does not change. Every step
        # is best-effort; on failure we still deliver the CRM harvest and a
        # marker so the flow can be tuned from real behavior.
        try:
            try:
                page.get_by_role("button", name="Filters").first.click(timeout=6000)
            except Exception:
                page.click("text=Filters", timeout=6000)
            page.wait_for_timeout(2500)
            picked = False
            # Tactic 1: a real <select> whose options include "AI Coach"
            try:
                for sel in page.query_selector_all("select"):
                    opts = [o.inner_text().strip() for o in
                            sel.query_selector_all("option")]
                    if any("AI Coach" in o for o in opts):
                        sel.select_option(label=[o for o in opts
                                                 if "AI Coach" in o][0])
                        picked = True
                        break
            except Exception:
                pass
            # Tactic 2: custom dropdown — click just below the CALLS TYPE
            # label to open it, then click the "AI Coach" option
            if not picked:
                box = page.get_by_text("CALLS TYPE", exact=False).first.bounding_box(
                    timeout=6000)
                if box:
                    page.mouse.click(box["x"] + box["width"] / 2,
                                     box["y"] + box["height"] + 28)
                    page.wait_for_timeout(900)
                    page.click("text=AI Coach", timeout=5000)
                    picked = True
            if not picked:
                raise RuntimeError("no dropdown tactic worked")
            page.wait_for_timeout(600)
            try:
                page.click("text=Apply Filters", timeout=5000)
            except Exception:
                page.click("button:has-text('Apply')", timeout=5000)
            page.wait_for_timeout(9000)
            rows2 = page.eval_on_selector_all(
                "table tr, [role='row']",
                "els => els.map(e => e.innerText.replace(/\\n/g, ' | '))")
            if rows2 and len(rows2) > 1:
                chunks.append("MAVERICK AI COACH ROWS\n" + "\n".join(rows2))
        except Exception as e:
            # Diagnose instead of guessing: what does the page actually look
            # like after the Filters click?
            diag = []
            try:
                html_src = page.content()
                btxt = ""
                try:
                    btxt = page.inner_text("body")
                except Exception:
                    pass
                diag.append("dialogs=%d" % len(page.query_selector_all(
                    "[role='dialog'], [class*='modal'], [class*='Modal'], "
                    "[class*='drawer'], [class*='Drawer']")))
                diag.append("iframes=%d" % len(page.query_selector_all("iframe")))
                diag.append("html_has_CALLS_TYPE=%s" % ("CALLS TYPE" in html_src.upper()))
                diag.append("html_has_AI_Coach=%s" % ("AI COACH" in html_src.upper()))
                diag.append("text_has_Filters=%s" % ("FILTERS" in btxt.upper()))
                i = btxt.upper().find("FILTER")
                if i >= 0:
                    diag.append("around_filters=%r" % btxt[max(0, i - 120):i + 300])
                selects = page.query_selector_all("select")
                diag.append("selects=%d" % len(selects))
            except Exception as de:
                diag.append("diag_error=%s" % str(de)[:100])
            chunks.append("AI_COACH_HARVEST_FAILED: %s || DIAG: %s"
                          % (str(e)[:200], " | ".join(diag)[:2200]))

        final_url = page.url
        ctx.close()
    return final_url, body_text, chunks


def run(debug=False):
    cfg = _load_config()
    url = cfg.get("call_log_url")
    if not url:
        print("No config. Run: python3 maverick_courier.py --setup")
        sys.exit(1)
    if debug:
        _harvest(url, headless=False, debug=True)
        return

    # Maverick fingerprints headless browsers (headed works, headless bounces
    # to login with the same profile). Try stealth-headless first; on a
    # bounce, retry HEADED — this Mac is always on and logged in, so a brief
    # visible window at harvest time is fine.
    final_url, body_text, chunks = _harvest(url, headless=True)
    if _looks_logged_out(final_url, body_text):
        print("headless bounced; retrying with a visible window...")
        final_url, body_text, chunks = _harvest(url, headless=False)

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
    ap.add_argument("--debug", action="store_true",
                    help="visible browser, prints what the harvest sees, posts nothing")
    args = ap.parse_args()
    if args.setup:
        setup()
    elif args.debug:
        run(debug=True)
    elif args.run:
        run()
    else:
        ap.print_help()
