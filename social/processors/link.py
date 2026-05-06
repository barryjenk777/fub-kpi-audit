"""Link handler. YouTube, Zoom recordings, generic URLs.

Treats a `links.txt` file in the inbox as a list of URLs (one per line).
For YouTube: pulls metadata + transcript via yt-dlp.
For Zoom share links: marked for manual transcript drop (Zoom doesn't expose).
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def classify(file_meta: dict) -> bool:
    name = file_meta.get("title", "").lower()
    return name in {"links.txt", "urls.txt"} or name.endswith(".url")


def youtube_metadata(url: str) -> dict | None:
    try:
        result = subprocess.run(
            ["yt-dlp", "--no-warnings", "--skip-download", "--print", "%(.{id,title,duration,description})j", url],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            import json as _json
            return _json.loads(result.stdout.strip().splitlines()[0])
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        return None
    return None


def youtube_transcript(url: str, out_dir: Path) -> Path | None:
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            ["yt-dlp", "--write-auto-sub", "--sub-lang", "en", "--skip-download",
             "--sub-format", "vtt", "-o", str(out_dir / "%(id)s.%(ext)s"), url],
            check=True, timeout=120, capture_output=True,
        )
        for f in out_dir.glob("*.en.vtt"):
            return f
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None
    return None


def treatment(file_meta: dict) -> dict:
    return {
        "kind": "link",
        "outputs": ["linkedin_long", "ig_carousel", "threads_thread", "facebook"],
        "needs_transcript": True,
    }
