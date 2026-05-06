"""Config loader for the social engine.

Reads credentials in this order of precedence:
    1. Environment variables (for Railway production)
    2. ~/.config/blotato/credentials.json (for local Mac dev)

Env var names:
    BLOTATO_API_KEY
    BLOTATO_INSTAGRAM_ACCOUNT_ID, BLOTATO_THREADS_ACCOUNT_ID,
    BLOTATO_FACEBOOK_ACCOUNT_ID, BLOTATO_LINKEDIN_ACCOUNT_ID, ...
    DRIVE_INBOX_ID, DRIVE_DRAFTS_ID, DRIVE_PUBLISHED_ID, DRIVE_IG_QUEUE_ID
    ANTHROPIC_API_KEY (consumed by anthropic SDK directly)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

CRED_PATH = Path.home() / ".config" / "blotato" / "credentials.json"

PLATFORMS = ["instagram", "threads", "facebook", "linkedin",
             "twitter", "tiktok", "youtube", "pinterest", "bluesky"]


def _file_creds() -> dict:
    if CRED_PATH.exists():
        try:
            return json.loads(CRED_PATH.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def load() -> dict:
    """Merge env vars and file credentials into a single dict.

    Env vars win over file values. Returns the merged dict.
    """
    file_data = _file_creds()
    out = dict(file_data)

    # API keys
    if os.environ.get("BLOTATO_API_KEY"):
        out["api_key"] = os.environ["BLOTATO_API_KEY"]
    if os.environ.get("ANTHROPIC_API_KEY"):
        out["anthropic_api_key"] = os.environ["ANTHROPIC_API_KEY"]

    # Per-platform account IDs
    for plat in PLATFORMS:
        env_key = f"BLOTATO_{plat.upper()}_ACCOUNT_ID"
        if os.environ.get(env_key):
            out[f"{plat}_account_id"] = os.environ[env_key]

    # Drive folder IDs
    for name in ["inbox", "drafts", "published", "ig_queue"]:
        env_key = f"DRIVE_{name.upper()}_ID"
        if os.environ.get(env_key):
            out[f"drive_{name}_id"] = os.environ[env_key]

    return out


def folder_id(name: str) -> str:
    """name in {'inbox', 'drafts', 'published', 'ig_queue'}."""
    creds = load()
    key = f"drive_{name}_id"
    val = creds.get(key)
    if not val:
        raise KeyError(f"{key} not in credentials (env var DRIVE_{name.upper()}_ID or credentials.json)")
    return val


def connected_platforms() -> list[str]:
    """Platforms with an account_id present in credentials or env."""
    creds = load()
    return [p for p in PLATFORMS if creds.get(f"{p}_account_id")]


REPO_ROOT = Path(__file__).resolve().parent.parent
BRIEF_PATH = REPO_ROOT / "briefs" / "ig_voice_guide.md"
LOCAL_DRAFTS_DIR = Path("/tmp/ig-queue/social_drafts")
LOCAL_DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
