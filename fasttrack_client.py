"""Client for syncing an agent's goal data to the Fast Track course
(legacyhometeamtraining.com).

One job: POST the agent's goal payload to Fast Track's /api/cc/sync-goal and
return the parsed response (which includes the signed magic link to email the
agent). Raises on any non-200 or malformed response so the caller can enqueue a
retry. Never build the magic link yourself — always use what the endpoint returns.
"""
import logging

import requests

import config

logger = logging.getLogger("fasttrack")


def sync_agent_to_fast_track(payload: dict, timeout: int = 15) -> dict:
    """POST an agent's goal payload to Fast Track.

    payload MUST include `email` and `name`; every other field is optional and
    Fast Track stores only what it receives, leaving the rest alone.

    Returns the response dict: {ok, agent_id, created, goal_fields_saved,
    magic_link}. Raises RuntimeError/ValueError on failure.
    """
    if not payload.get("email") or not payload.get("name"):
        raise ValueError("Fast Track sync requires email and name")

    resp = requests.post(
        config.FAST_TRACK_SYNC_URL,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "x-cc-secret": config.FAST_TRACK_SYNC_SECRET,
        },
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Fast Track sync HTTP {resp.status_code}: {resp.text[:300]}"
        )

    data = resp.json()
    if not data.get("ok") or not data.get("magic_link"):
        raise RuntimeError(f"Fast Track sync bad response: {str(data)[:300]}")

    logger.info(
        "[fast-track sync] email=%s ftId=%s created=%s saved=%s",
        payload.get("email"), data.get("agent_id"), data.get("created"),
        ",".join(data.get("goal_fields_saved") or []),
    )
    return data


# ---------------------------------------------------------------------------
# Progress read path (the onboarding board)
# ---------------------------------------------------------------------------
# Fast Track exposes two read-only GETs behind the same x-cc-secret header the
# sync uses. The roster answers "where is everybody" in one call; the per-agent
# lookup backs the drilldown. Progress is NEVER persisted here — Fast Track is
# the source of truth and we read it live (with a short cache).

import time as _time

_FT_BASE = "https://www.legacyhometeamtraining.com"
_ROSTER_TTL_SECONDS = 60
_roster_cache = {"data": None, "fetched_at": 0.0}


def _ft_secret():
    return getattr(config, "FAST_TRACK_SYNC_SECRET", "") or ""


def fetch_fast_track_roster(timeout: int = 10, force: bool = False) -> dict:
    """Full Fast Track roster, cached 60s. Raises on non-2xx so the caller can
    fall back to the last good payload. force=True bypasses the cache."""
    now = _time.time()
    if not force and _roster_cache["data"] is not None:
        if now - _roster_cache["fetched_at"] < _ROSTER_TTL_SECONDS:
            out = dict(_roster_cache["data"])
            out["_cache_age_s"] = int(now - _roster_cache["fetched_at"])
            return out
    r = requests.get(f"{_FT_BASE}/api/cc/progress",
                     headers={"x-cc-secret": _ft_secret()}, timeout=timeout)
    if r.status_code == 403:
        logger.error("[fast-track progress] 403 forbidden — FAST_TRACK_SYNC_SECRET wrong or missing")
    r.raise_for_status()
    data = r.json()
    logger.debug("[fast-track progress] roster ok, %d agents", len(data.get("agents") or []))
    _roster_cache["data"] = data
    _roster_cache["fetched_at"] = now
    data = dict(data)
    data["_cache_age_s"] = 0
    return data


def fetch_fast_track_agent(email: str, timeout: int = 10):
    """One agent by email. Returns the agent dict, or None if Fast Track has no
    row for them (404). Never cached — the drilldown must read live."""
    e = (email or "").lower().strip()
    if not e:
        return None
    r = requests.get(f"{_FT_BASE}/api/cc/progress", params={"email": e},
                     headers={"x-cc-secret": _ft_secret()}, timeout=timeout)
    if r.status_code == 404:
        logger.debug("[fast-track progress] no row for %s", e)
        return None
    if r.status_code == 403:
        logger.error("[fast-track progress] 403 forbidden on agent lookup")
    r.raise_for_status()
    return r.json().get("agent")


def clear_roster_cache():
    """Drop the cached roster so the next fetch hits Fast Track (Refresh button)."""
    _roster_cache["data"] = None
    _roster_cache["fetched_at"] = 0.0
