"""
Merit Routing v1 — measurement first.

DOCTRINE (Barry's ground-truth doc, sacred):
  Rotation and eligibility are BINARY and earned weekly. Merit NEVER
  decides who is eligible. Merit only decides the ORDER / matching
  INSIDE the already-earned eligible set. If merit data is thin, we
  fall back to round-robin, we never shrink or grow the pool.

What lives here:
  compute_merit_scorecard()  — per active agent, per lead-type lane,
                               trailing 180 days where data allows
  refresh_scorecard()        — compute + cache in app_state
  get_cached_scorecard()     — read the cache (no API calls)
  rank_lane_agents()         — merit ordering WITHIN a candidate set
  build_advisory_email()     — Monday probate advisory for Barry

Honesty rules (no fabrication):
  Every metric carries a 'basis' saying exactly where the number came
  from. Anything we cannot compute cheaply is skipped and says so.
  Lanes with fewer than MERIT_MIN_LEADS leads render as not enough
  data yet and are NEVER used for routing.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import config
import db as _db

logger = logging.getLogger("merit")

MERIT_WINDOW_DAYS = 180
MERIT_MIN_LEADS   = 8          # volume guard: below this a lane is not routable
MERIT_STATE_KEY   = "merit_scorecard_v1"

# Lane order is also display order in the advisory email.
LANES = ("transfer", "probate", "zbuyer", "ylopo", "sphere")

_LANE_LABELS = {
    "transfer": "ISA transfers",
    "probate":  "Probate",
    "zbuyer":   "Zbuyer",
    "ylopo":    "Ylopo",
    "sphere":   "Sphere and referral",
}


def _strip(text):
    """All outbound copy goes through coach_voice._strip_dashes."""
    try:
        import coach_voice as _cv
        return _cv._strip_dashes(text)
    except Exception:
        return text


def lane_for_source(source):
    """Map a FUB source string to a merit lane. None = no lane."""
    s = (source or "").lower()
    if not s:
        return None
    if "probate" in s:
        return "probate"
    if "zbuyer" in s:
        return "zbuyer"
    if "ylopo" in s:
        return "ylopo"
    if "sphere" in s or "referral" in s:
        return "sphere"
    return None


def lane_for_lead(source, is_transfer=False):
    """Lane for a single lead at routing time. Transfer history wins,
    then source mapping. None = unmapped, caller falls back to round-robin."""
    if is_transfer:
        return "transfer"
    return lane_for_source(source)


# ---------------------------------------------------------------------------
# Scorecard computation
# ---------------------------------------------------------------------------

def compute_merit_scorecard(client=None):
    """
    Build the merit scorecard: per active agent (minus config.EXCLUDED_USERS),
    per lane, trailing MERIT_WINDOW_DAYS where the data allows.

    Data sources (each metric says which in its 'basis'):
      leads_touched  — one bulk FUB people pull (created in window), grouped
                       by assignedTo + source. Transfer lane counts come from
                       the local isa_transfers table instead.
      appts_set/met  — local appointments table (synced from FUB), grouped
                       by agent + source.
      transfer_conversion — isa_transfers joined to local appointments: a
                       transfer converts when an appointment was logged for
                       that lead after the transfer date. The FUB tag
                       ISA_TRANSFER_SUCCESSFUL is not stored per lead in the
                       local DB, so appointment follow-through is the honest
                       success signal we actually have.
      convo_rate     — skipped: would need one API call per lead.
    """
    if client is None:
        from fub_client import FUBClient
        client = FUBClient()

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=MERIT_WINDOW_DAYS)
    excluded = set(config.EXCLUDED_USERS)
    agent_names = sorted(
        p["agent_name"] for p in (_db.get_agent_profiles(active_only=True) or [])
        if p.get("agent_name") and p["agent_name"] not in excluded
    )

    # 1. Bulk people pull, grouped client-side (one paginated fetch, weekly).
    leads = {a: {ln: 0 for ln in LANES} for a in agent_names}
    people_pulled = 0
    try:
        people = client.get_people(created_since=window_start)
    except Exception as e:
        logger.warning("merit: bulk people pull failed: %s", e)
        people = []
    for p in people or []:
        people_pulled += 1
        agent = (p.get("assignedTo") or "").strip()
        if agent not in leads:
            continue
        ln = lane_for_source(p.get("source"))
        if ln:
            leads[agent][ln] += 1

    # 2. Local appointments by agent + source lane.
    appts = {a: {ln: {"set": 0, "met": 0} for ln in LANES} for a in agent_names}
    for row in _db.get_appointment_lane_counts(days=MERIT_WINDOW_DAYS):
        agent = row.get("agent_name")
        if agent not in appts:
            continue
        ln = lane_for_source(row.get("source"))
        if ln:
            appts[agent][ln]["set"] += int(row.get("appts_set") or 0)
            appts[agent][ln]["met"] += int(row.get("appts_met") or 0)

    # 3. ISA transfer lane from local isa_transfers.
    transfers = {r["agent_name"]: r for r in _db.get_transfer_merit_counts(days=MERIT_WINDOW_DAYS)
                 if r.get("agent_name") in leads}

    basis_leads = f"FUB people created in trailing {MERIT_WINDOW_DAYS} days, grouped by assigned agent and source"
    basis_appts = "local appointments table synced from FUB, matched by agent and lead source"
    basis_xfer  = ("local isa_transfers table; a transfer counts as converted when an appointment "
                   "was logged for that lead after the transfer date (the ISA_TRANSFER_SUCCESSFUL "
                   "tag is not stored per lead locally, so appointment follow-through is the "
                   "success signal)")
    basis_convo = "skipped, computing it would need one FUB API call per lead"

    agents_out = {}
    for a in agent_names:
        lanes_out = {}
        for ln in LANES:
            if ln == "transfer":
                t = transfers.get(a) or {}
                touched = int(t.get("transfers") or 0)
                converted = int(t.get("with_appt") or 0)
                conv = round(converted / touched, 3) if touched else None
                lane_d = {
                    "leads_touched": touched,
                    "appts_set": converted,
                    "appts_met": None,
                    "transfer_conversion": conv,
                    "convo_rate": None,
                    "merit_metric": conv,
                    "merit_metric_name": "transfer_conversion",
                    "basis": {
                        "leads_touched": f"isa_transfers rows for this agent in trailing {MERIT_WINDOW_DAYS} days (no_answer excluded)",
                        "appts_set": basis_xfer,
                        "transfer_conversion": basis_xfer,
                        "convo_rate": basis_convo,
                    },
                }
            else:
                touched = leads[a][ln]
                a_set = appts[a][ln]["set"]
                a_met = appts[a][ln]["met"]
                per_lead = round(a_set / touched, 3) if touched else None
                lane_d = {
                    "leads_touched": touched,
                    "appts_set": a_set,
                    "appts_met": a_met,
                    "convo_rate": None,
                    "merit_metric": per_lead,
                    "merit_metric_name": "appts_set_per_lead",
                    "basis": {
                        "leads_touched": basis_leads,
                        "appts_set": basis_appts,
                        "appts_met": basis_appts,
                        "convo_rate": basis_convo,
                    },
                }
            lane_d["sufficient"] = bool(lane_d["leads_touched"] >= MERIT_MIN_LEADS)
            if not lane_d["sufficient"]:
                lane_d["note"] = "not enough data yet"
            lanes_out[ln] = lane_d
        agents_out[a] = {"lanes": lanes_out}

    # Team rollup per lane (sufficient agents only — the routable set).
    team = {}
    for ln in LANES:
        rows = []
        for a in agent_names:
            d = agents_out[a]["lanes"][ln]
            if d["sufficient"] and d["merit_metric"] is not None:
                rows.append((a, d["merit_metric"]))
        rows.sort(key=lambda x: (-x[1], x[0]))
        team[ln] = {
            "sufficient_agents": [a for a, _ in rows],
            "avg_metric": round(sum(m for _, m in rows) / len(rows), 3) if rows else None,
        }

    return {
        "computed_at": now.isoformat(),
        "window_days": MERIT_WINDOW_DAYS,
        "min_leads": MERIT_MIN_LEADS,
        "people_pulled": people_pulled,
        "agents": agents_out,
        "team": team,
        "doctrine": ("merit orders within the earned pool, never expands or shrinks it; "
                     "insufficient lanes are never used for routing"),
    }


def refresh_scorecard(client=None):
    """Compute and cache the scorecard. Returns it."""
    sc = compute_merit_scorecard(client=client)
    try:
        _db.set_app_state(MERIT_STATE_KEY, json.dumps(sc))
    except Exception as e:
        logger.warning("merit: cache write failed: %s", e)
    return sc


def get_cached_scorecard():
    """Cached scorecard from app_state, or None. Zero API calls."""
    try:
        val, _ = _db.get_app_state(MERIT_STATE_KEY)
        if not val:
            return None
        sc = json.loads(val)
        return sc if isinstance(sc, dict) and sc.get("agents") else None
    except Exception as e:
        logger.warning("merit: cache read failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Ordering (used by the Phoenix sweep)
# ---------------------------------------------------------------------------

def rank_lane_agents(scorecard, lane, candidate_names):
    """
    Order candidate_names by merit for one lane, best first.

    DOCTRINE: candidates arrive already eligible (they earned it). This
    function never adds or removes a candidate, it only sorts the subset
    that has SUFFICIENT data in the lane. Candidates without sufficient
    lane data are simply absent from the result, and the caller round-robins
    them exactly as before.

    Returns [(agent_name, metric), ...] sorted metric desc, name asc.
    """
    if not scorecard or not lane:
        return []
    out = []
    for name in candidate_names or []:
        lane_d = ((scorecard.get("agents") or {}).get(name) or {}).get("lanes", {}).get(lane)
        if lane_d and lane_d.get("sufficient") and lane_d.get("merit_metric") is not None:
            out.append((name, float(lane_d["merit_metric"])))
    out.sort(key=lambda x: (-x[1], x[0]))
    return out


# ---------------------------------------------------------------------------
# Probate advisory email (Monday, only when probate lane has real data)
# ---------------------------------------------------------------------------

def _fmt_metric(lane, d):
    if lane == "transfer":
        return f"{int(round((d.get('transfer_conversion') or 0) * 100))}% of {d['leads_touched']} transfers reached an appointment"
    return f"{d['appts_set']} appointments from {d['leads_touched']} leads ({d.get('merit_metric') or 0:.2f} per lead)"


def build_advisory_email(scorecard):
    """
    Build (subject, html, text) for Barry's Monday merit read.
    Returns None unless the probate lane has at least one sufficient agent.
    Plain Barry voice, no dashes, nothing invented: every line is a number
    we computed with a stated basis.
    """
    if not scorecard:
        return None
    team = scorecard.get("team") or {}
    probate_agents = (team.get("probate") or {}).get("sufficient_agents") or []
    if not probate_agents:
        return None

    agents = scorecard.get("agents") or {}
    lines = []
    lines.append("Here is the merit read from the trailing "
                 f"{scorecard.get('window_days', MERIT_WINDOW_DAYS)} days. "
                 "Reminder on the doctrine: this never changes who is eligible. "
                 "It only tells us who converts what, so ordering inside the "
                 "earned pool gets smarter.")
    lines.append("")

    insufficient = []
    for ln in LANES:
        suff = (team.get(ln) or {}).get("sufficient_agents") or []
        if not suff:
            insufficient.append(ln)
            continue
        lines.append(f"{_LANE_LABELS[ln]}:")
        for name in suff[:2]:
            d = agents[name]["lanes"][ln]
            basis_key = "transfer_conversion" if ln == "transfer" else "appts_set"
            lines.append(f"  {name}: {_fmt_metric(ln, d)}. Basis: {d['basis'][basis_key]}.")
        lines.append("")

    top = probate_agents[0]
    top_d = agents[top]["lanes"]["probate"]
    avg = (team.get("probate") or {}).get("avg_metric")
    avg_txt = f"{avg:.2f}" if avg is not None else "n/a"
    lines.append(f"Next probate assignment: {top}, "
                 f"{(top_d.get('merit_metric') or 0):.2f} appointments per probate lead "
                 f"vs team {avg_txt}.")
    lines.append("")

    if insufficient:
        need = scorecard.get("min_leads", MERIT_MIN_LEADS)
        pretty = ", ".join(_LANE_LABELS[ln] for ln in insufficient)
        lines.append(f"Lanes still warming up: {pretty}. Each needs at least "
                     f"{need} leads on one agent inside the window before merit "
                     "can order it. Until then those lanes route round-robin, "
                     "exactly as before.")

    text = _strip("\n".join(lines))
    html = "<p>" + text.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>"
    subject = _strip("Merit read: who converts what")
    return subject, html, text
