"""Blotato client — upload media + publish across platforms.

Credentials live at ~/.config/blotato/credentials.json (chmod 600).
Schema: {"api_key": "...", "instagram_account_id": "...", "threads_account_id": "...",
         "facebook_account_id": "...", "linkedin_account_id": "...", ...}

Platform map: each key like "instagram_account_id" maps to platform "instagram".
Blotato platform IDs: instagram, threads, facebook, linkedin, twitter, tiktok,
youtube, pinterest, bluesky.

CLI:
    python blotato_client.py upload <image_url>
    python blotato_client.py publish <platform> "<caption>" <hosted_url> [<hosted_url> ...]
    python blotato_client.py post-from-url <platform> <image_url> "<caption>"
    python blotato_client.py post-multi "<caption>" <hosted_url> -p instagram threads facebook
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

BASE_URL = "https://backend.blotato.com/v2"
CRED_PATH = Path.home() / ".config" / "blotato" / "credentials.json"

KNOWN_PLATFORMS = {
    "instagram", "threads", "facebook", "linkedin",
    "twitter", "tiktok", "youtube", "pinterest", "bluesky",
}


def _load_creds() -> dict:
    """Merge env vars and credentials.json. Env wins. Used for both local and Railway."""
    creds = {}
    if CRED_PATH.exists():
        try:
            creds = json.loads(CRED_PATH.read_text())
        except json.JSONDecodeError:
            creds = {}
    if os.environ.get("BLOTATO_API_KEY"):
        creds["api_key"] = os.environ["BLOTATO_API_KEY"]
    for plat in KNOWN_PLATFORMS:
        env_var = f"BLOTATO_{plat.upper()}_ACCOUNT_ID"
        if os.environ.get(env_var):
            creds[f"{plat}_account_id"] = os.environ[env_var]
    if not creds.get("api_key"):
        sys.exit(f"No api_key. Set BLOTATO_API_KEY env var or write {CRED_PATH}.")
    return creds


def _account_id(creds: dict, platform: str) -> str:
    key = f"{platform}_account_id"
    val = creds.get(key)
    if not val:
        sys.exit(
            f"No account ID for '{platform}'. Set BLOTATO_{platform.upper()}_ACCOUNT_ID env var "
            f"or add '{key}' to {CRED_PATH}."
        )
    return val


def _headers(api_key: str) -> dict:
    return {"blotato-api-key": api_key, "Content-Type": "application/json"}


def upload_media(image_url: str) -> str:
    """POST /v2/media — re-host a public URL on Blotato's CDN."""
    creds = _load_creds()
    r = requests.post(
        f"{BASE_URL}/media",
        headers=_headers(creds["api_key"]),
        json={"url": image_url},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    hosted = data.get("url") or data.get("mediaUrl") or data.get("hostedUrl")
    if not hosted:
        sys.exit(f"Unexpected upload response: {data}")
    return hosted


def publish(platform: str, text: str, media_urls: list[str], account_id: str | None = None) -> dict:
    """POST /v2/posts — publish to a single platform."""
    if platform not in KNOWN_PLATFORMS:
        sys.exit(f"Unknown platform '{platform}'. Known: {sorted(KNOWN_PLATFORMS)}")
    creds = _load_creds()
    acct = account_id or os.environ.get(f"BLOTATO_{platform.upper()}_ACCOUNT_ID") or _account_id(creds, platform)
    body = {
        "post": {
            "accountId": acct,
            "content": {
                "text": text,
                "mediaUrls": media_urls,
                "platform": platform,
            },
            "target": {"targetType": platform},
        }
    }
    r = requests.post(
        f"{BASE_URL}/posts",
        headers=_headers(creds["api_key"]),
        json=body,
        timeout=60,
    )
    if not r.ok:
        return {"error": True, "status": r.status_code, "body": r.text, "platform": platform}
    return {"platform": platform, **r.json()}


def publish_multi(platform_to_text: dict[str, str], media_urls: list[str]) -> dict[str, dict]:
    """Publish a per-platform map of captions to all listed platforms.

    Each platform can have a different caption (LinkedIn long, Threads short, etc.).
    Media URLs are shared across platforms (Blotato will re-encode as needed).
    Returns {platform: response_or_error}.
    """
    results = {}
    for platform, text in platform_to_text.items():
        results[platform] = publish(platform, text, media_urls)
    return results


# Backwards-compat alias used by older code
def publish_instagram(text: str, media_urls: list[str], account_id: str | None = None) -> dict:
    return publish("instagram", text, media_urls, account_id)


def post_from_url(platform: str, image_url: str, text: str) -> dict:
    """Convenience: upload a public image URL, then publish to one platform."""
    hosted = upload_media(image_url)
    return publish(platform, text, [hosted])


def _main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("upload")
    up.add_argument("image_url")

    pb = sub.add_parser("publish")
    pb.add_argument("platform")
    pb.add_argument("text")
    pb.add_argument("media_urls", nargs="+")

    pf = sub.add_parser("post-from-url")
    pf.add_argument("platform")
    pf.add_argument("image_url")
    pf.add_argument("text")

    pm = sub.add_parser("post-multi")
    pm.add_argument("text")
    pm.add_argument("media_url")
    pm.add_argument("-p", "--platforms", nargs="+", required=True)

    args = p.parse_args()

    if args.cmd == "upload":
        print(upload_media(args.image_url))
    elif args.cmd == "publish":
        print(json.dumps(publish(args.platform, args.text, args.media_urls), indent=2))
    elif args.cmd == "post-from-url":
        print(json.dumps(post_from_url(args.platform, args.image_url, args.text), indent=2))
    elif args.cmd == "post-multi":
        hosted = upload_media(args.media_url)
        plat_map = {p: args.text for p in args.platforms}
        print(json.dumps(publish_multi(plat_map, [hosted]), indent=2))


if __name__ == "__main__":
    _main()
