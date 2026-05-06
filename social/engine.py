"""Social engine: scan inbox, classify, scaffold draft posts.

Captions are NOT written here. Captions are written by Claude in conversation
when Barry runs `/process-inbox`. This module produces the empty per-platform
draft structures that Claude fills in.

CLI:
    python -m social.engine scan          # list inbox files (no side effects)
    python -m social.engine bootstrap     # create draft JSON for any inbox file not yet in drafts
    python -m social.engine list-drafts   # show drafts and status
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid
from pathlib import Path

from . import config, storage
from .processors import photo, audio, video, link, note

PROCESSORS = [photo, audio, video, link, note]
DRAFTS_DIR = config.LOCAL_DRAFTS_DIR  # legacy local path; storage.py is now the source of truth


def classify(file_meta: dict):
    for p in PROCESSORS:
        if p.classify(file_meta):
            return p
    return None


def week_bucket(when: dt.date | None = None) -> str:
    when = when or dt.date.today()
    iso_year, iso_week, _ = when.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def draft_path(week: str, draft_id: str) -> Path:
    week_dir = DRAFTS_DIR / week
    week_dir.mkdir(parents=True, exist_ok=True)
    return week_dir / f"{draft_id}.json"


def make_skeleton(file_meta: dict, treatment_data: dict) -> dict:
    draft_id = uuid.uuid4().hex[:10]
    platforms = {p: {"caption": "", "status": "draft", "submission_id": None, "scheduled_for": None}
                 for p in ["instagram", "threads", "facebook", "linkedin"]}
    return {
        "id": draft_id,
        "iso_week": week_bucket(),
        "created_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": {
            "drive_file_id": file_meta["id"],
            "drive_file_name": file_meta.get("title"),
            "mime": file_meta.get("mimeType"),
            "drive_view_url": file_meta.get("viewUrl"),
            "kind": treatment_data["kind"],
        },
        "treatment": treatment_data,
        "media": {
            "local_path": None,
            "hosted_url": None,
            "blotato_hosted_url": None,
        },
        "platforms": platforms,
    }


def list_existing_drafts() -> dict[str, dict]:
    """Return all drafts keyed by source.drive_file_id."""
    out = {}
    for d in storage.list_all():
        try:
            out[d["source"]["drive_file_id"]] = d
        except KeyError:
            continue
    return out


def save_draft(draft: dict, week: str | None = None) -> None:
    if week:
        draft["iso_week"] = week
    storage.save(draft)


def bootstrap_from_inbox(inbox_files: list[dict]) -> list[dict]:
    """Create draft skeletons for any inbox file not already drafted."""
    existing = list_existing_drafts()
    new_drafts = []
    for f in inbox_files:
        if f["id"] in existing:
            continue
        proc = classify(f)
        if not proc:
            continue
        treatment_data = proc.treatment(f)
        draft = make_skeleton(f, treatment_data)
        save_draft(draft)
        new_drafts.append(draft)
    return new_drafts


def _cli():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("scan")
    sub.add_parser("bootstrap")
    sub.add_parser("list-drafts")
    args = p.parse_args()

    if args.cmd == "scan":
        print("Scan requires GDrive MCP context — run from /process-inbox slash command.")
        print(f"Inbox folder ID: {config.folder_id('inbox')}")
        print(f"Drafts dir: {DRAFTS_DIR}")
    elif args.cmd == "bootstrap":
        print("Bootstrap requires inbox metadata input via stdin (JSON list of file objects).")
        try:
            files = json.load(sys.stdin)
        except (json.JSONDecodeError, ValueError):
            sys.exit("Pipe a JSON list of GDrive file metadata to stdin.")
        new = bootstrap_from_inbox(files)
        print(f"Created {len(new)} new drafts.")
        for d in new:
            print(f"  - {d['id']} ({d['source']['kind']}) {d['source']['drive_file_name']}")
    elif args.cmd == "list-drafts":
        drafts = list_existing_drafts()
        if not drafts:
            print(f"No drafts in {DRAFTS_DIR}")
            return
        for src_id, d in drafts.items():
            statuses = {p: v["status"] for p, v in d["platforms"].items()}
            print(f"{d['id']}  {d['source']['kind']:8}  {d['source']['drive_file_name']:30}  {statuses}")


if __name__ == "__main__":
    _cli()
