"""Callingly API client (Nov 7 2026).

Barry's Callingly account rings agents' actual phones the moment a paid
lead lands; the first agent to press 1 is bridged live to the lead.
This client is the pipe between Command Center's routing and that ring.

API facts (help.callingly.com article 38, verified Nov 7 2026):
- Auth: Bearer token from Settings -> API Keys, header Accept: json.
- POST /v1/calls with team_id + lead fields triggers the ring. crm_id
  carries our FUB person id so webhooks map back to the lead.
- WHO rings is a property of the TEAM (members, priority, cap), never
  of the call: control the roster with PUT /v1/teams/{id}/agents.
- Webhook event call_completed (filters: direction, status, team) POSTs
  to a target_url. Payload shape is undocumented and unsigned, so our
  receiving URL carries its own secret and the handler re-reads the
  call from the API for truth instead of trusting the body.
- Webhooks auto-disable after 100 consecutive failures: the receiver
  must always answer 200 fast.

SAFETY: every function here is either a GET or an explicitly
Barry-triggered configuration write. Nothing in this module rings a
phone unless the desk's callingly_live gate is on or Barry hits the
test endpoint with a number he typed himself.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)

BASE = "https://api.callingly.com/v1"
TIMEOUT = 15


def _key():
    return (os.environ.get("CALLINGLY_API_KEY") or "").strip()


def is_configured():
    return bool(_key())


def _req(method, path, json_data=None, params=None):
    if not _key():
        raise RuntimeError("CALLINGLY_API_KEY not set")
    r = requests.request(
        method, BASE + path,
        headers={"Authorization": "Bearer %s" % _key(),
                 "Accept": "application/json"},
        json=json_data, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {"raw": r.text[:500]}


# ── Read side (safe any time) ────────────────────────────────────────────
def get_teams():
    return _req("GET", "/teams")


def get_team_agents(team_id):
    return _req("GET", "/teams/%s/agents" % team_id)


def list_agents():
    return _req("GET", "/agents")


def get_call(call_id):
    return _req("GET", "/calls/%s" % call_id)


def list_calls(start, end, team_id=None, limit=50):
    params = {"start": start, "end": end, "limit": limit}
    if team_id:
        params["team_id"] = team_id
    return _req("GET", "/calls", params=params)


def list_webhooks():
    return _req("GET", "/webhooks")


# ── Write side (explicit admin actions or the gated live lane) ──────────
def create_call(team_id, first_name, last_name, phone_number,
                fub_person_id, source, email=None):
    """Trigger the ring for one lead. The only function that makes
    phones ring; callers own the gating."""
    body = {"team_id": int(team_id),
            "first_name": first_name or "Lead",
            "last_name": last_name or "",
            "phone_number": phone_number,
            "source": source,
            "crm_id": int(fub_person_id)}
    if email:
        body["email"] = email
    return _req("POST", "/calls", json_data=body)


def set_team_agents(team_id, agent_ids):
    """Replace the ring roster for a team (explicit admin action)."""
    return _req("PUT", "/teams/%s/agents" % team_id,
                json_data={"agents": [str(a) for a in agent_ids]})


def create_webhook(name, target_url, team_id=None):
    """Register our call_completed webhook (explicit admin action)."""
    body = {"name": name, "event": "call_completed",
            "target_url": target_url, "call_direction": "outbound"}
    if team_id:
        body["team_id"] = int(team_id)
    return _req("POST", "/webhooks", json_data=body)
