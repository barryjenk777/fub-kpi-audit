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
