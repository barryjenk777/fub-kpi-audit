"""Storage backend for social drafts.

Two backends:
- Postgres (when DATABASE_URL is set): drafts stored in social_drafts table.
- Local files (fallback): drafts stored as JSON in /tmp/ig-queue/social_drafts/<week>/<id>.json.

The Postgres backend is the production path: lets the local /process-inbox
slash command write drafts that the deployed Railway app can read for review.
For this to work, Barry sets DATABASE_URL on his Mac to point at the Railway
Postgres instance (one shell line in ~/.zshrc).

Both backends expose the same API: list_all() / get(id) / save(draft) / delete(id).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from . import config

LOCAL_DIR = config.LOCAL_DRAFTS_DIR


def _db():
    """Lazy import of db module so the local Python 3.9 dev environment
    isn't forced to import db.py (which uses 3.10+ type hints)."""
    sys.path.insert(0, str(config.REPO_ROOT))
    import db as _module  # noqa: E402
    return _module


def _use_postgres() -> bool:
    if not os.environ.get("DATABASE_URL"):
        return False
    try:
        return _db().is_available()
    except Exception:
        return False


# -------- Postgres backend --------

def _pg_save(draft: dict) -> None:
    with _db().get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO social_drafts (id, drive_file_id, iso_week, data, updated_at)
                VALUES (%s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW()
                """,
                (draft["id"], draft["source"]["drive_file_id"],
                 draft.get("iso_week", ""), json.dumps(draft)),
            )


def _pg_get(draft_id: str) -> dict | None:
    with _db().get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM social_drafts WHERE id = %s", (draft_id,))
            row = cur.fetchone()
            return row[0] if row else None


def _pg_get_by_drive_id(drive_file_id: str) -> dict | None:
    with _db().get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM social_drafts WHERE drive_file_id = %s", (drive_file_id,))
            row = cur.fetchone()
            return row[0] if row else None


def _pg_list_all() -> list[dict]:
    with _db().get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM social_drafts ORDER BY updated_at DESC")
            return [row[0] for row in cur.fetchall()]


def _pg_delete(draft_id: str) -> None:
    with _db().get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM social_drafts WHERE id = %s", (draft_id,))


# -------- Local file backend --------

def _file_path(draft: dict) -> Path:
    week = draft.get("iso_week") or "uncategorized"
    week_dir = LOCAL_DIR / week
    week_dir.mkdir(parents=True, exist_ok=True)
    return week_dir / f"{draft['id']}.json"


def _file_save(draft: dict) -> None:
    p = _file_path(draft)
    p.write_text(json.dumps(draft, indent=2))


def _file_iter_paths():
    for week_dir in LOCAL_DIR.glob("*"):
        if not week_dir.is_dir():
            continue
        for jf in week_dir.glob("*.json"):
            yield jf


def _file_get(draft_id: str) -> dict | None:
    for jf in _file_iter_paths():
        try:
            d = json.loads(jf.read_text())
            if d.get("id") == draft_id:
                return d
        except json.JSONDecodeError:
            continue
    return None


def _file_get_by_drive_id(drive_file_id: str) -> dict | None:
    for jf in _file_iter_paths():
        try:
            d = json.loads(jf.read_text())
            if d.get("source", {}).get("drive_file_id") == drive_file_id:
                return d
        except json.JSONDecodeError:
            continue
    return None


def _file_list_all() -> list[dict]:
    out = []
    for jf in _file_iter_paths():
        try:
            out.append(json.loads(jf.read_text()))
        except json.JSONDecodeError:
            continue
    out.sort(key=lambda d: d.get("created_at", ""), reverse=True)
    return out


def _file_delete(draft_id: str) -> None:
    for jf in _file_iter_paths():
        try:
            d = json.loads(jf.read_text())
            if d.get("id") == draft_id:
                jf.unlink()
                return
        except json.JSONDecodeError:
            continue


# -------- Public API --------

def save(draft: dict) -> None:
    """Persist a draft. Tries Postgres first, falls back to local files."""
    if _use_postgres():
        try:
            _pg_save(draft)
            return
        except Exception as e:
            print(f"[social.storage] Postgres save failed, falling back to file: {e}")
    _file_save(draft)


def get(draft_id: str) -> dict | None:
    if _use_postgres():
        try:
            d = _pg_get(draft_id)
            if d:
                return d
        except Exception as e:
            print(f"[social.storage] Postgres get failed: {e}")
    return _file_get(draft_id)


def get_by_drive_id(drive_file_id: str) -> dict | None:
    if _use_postgres():
        try:
            d = _pg_get_by_drive_id(drive_file_id)
            if d:
                return d
        except Exception as e:
            print(f"[social.storage] Postgres get_by_drive_id failed: {e}")
    return _file_get_by_drive_id(drive_file_id)


def list_all() -> list[dict]:
    """Return all drafts. Postgres if available; if both have data, Postgres wins."""
    if _use_postgres():
        try:
            return _pg_list_all()
        except Exception as e:
            print(f"[social.storage] Postgres list failed: {e}")
    return _file_list_all()


def delete(draft_id: str) -> None:
    if _use_postgres():
        try:
            _pg_delete(draft_id)
        except Exception as e:
            print(f"[social.storage] Postgres delete failed: {e}")
    _file_delete(draft_id)


def backend_in_use() -> str:
    return "postgres" if _use_postgres() else "files"
