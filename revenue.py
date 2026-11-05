"""Funnel and revenue (Barry, Nov 4 2026).

The money layer Grokbot asked for, built honestly: every number comes
from FUB deals, FUB pipelines, or the synced appointments table, and the
payload always carries coverage stats (how many deals have a price, a
commission, a person link) instead of invented figures. Probe findings
(Nov 4 2026): 65 deals synced from Dotloop, 29 with price, commissions
mostly null, person links mostly empty. The upstream fix is better deal
hygiene in Dotloop/FUB; this module reports what exists and says what is
missing.

Referral fee table (Barry, Nov 4 2026): Ylopo deals carry a 40% referral
fee to LPT (LPT pays the Ylopo bill). Probate leads carry 25%. zBuyer
and Realtor.com carry no referral fee (Realtor.com is a per-lead cost,
tracked by the ladder's Friday receipt).
"""

import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import db as _db

logger = logging.getLogger(__name__)

# Dotloop is also the team's ONBOARDING paperwork tool, and its FUB sync
# ships those loops over as "deals" (Barry, Nov 4 2026: "that's not a
# transaction"). They are ignored everywhere money is counted and
# surfaced in the TM Deal Desk only with a delete action.
ADMIN_DEAL_RE = re.compile(
    r"onboarding|commission split|handbook|independent contractor", re.I)


def is_admin_deal(deal):
    return bool(ADMIN_DEAL_RE.search(deal.get("name") or ""))

# source bucket -> referral fee fraction taken off GCI
REFERRAL_FEES = {
    "ylopo": 0.40,
    "probate": 0.25,
    "zbuyer": 0.0,
    "realtor.com": 0.0,
    "cash_site": 0.0,
    "soi_referral": 0.0,
    "other": 0.0,
}

_CACHE = {"at": 0.0, "data": None}
_CACHE_TTL = 30 * 60
_LOCK = threading.Lock()


def bucket_for(source):
    """Map a FUB source string to a revenue bucket."""
    s = (source or "").strip().lower()
    if not s:
        return "other"
    if "probate" in s:
        return "probate"
    if "zbuyer" in s:
        return "zbuyer"
    if "realtor.com" in s or s == "realtor":
        return "realtor.com"
    if "hamptonroadshome.cash" in s or "cash offer" in s:
        return "cash_site"
    if "ylopo" in s or "ppc" in s:
        return "ylopo"
    if "referral" in s or "sphere" in s or "soi" in s or "past client" in s:
        return "soi_referral"
    return "other"


def _parse_dt(v):
    try:
        return datetime.fromisoformat((v or "").replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _fetch_all_deals(client):
    deals, url_params = [], {"limit": 100}
    data = client._request("GET", "deals", params=url_params) or {}
    deals.extend(data.get("deals") or [])
    nxt = (data.get("_metadata") or {}).get("next")
    hops = 0
    while nxt and hops < 20:
        data = client._request("GET", "deals",
                               params={"limit": 100, "next": nxt}) or {}
        deals.extend(data.get("deals") or [])
        nxt = (data.get("_metadata") or {}).get("next")
        hops += 1
    return deals


def _stage_map(client):
    out = {}
    data = client._request("GET", "pipelines", params={"limit": 50}) or {}
    for p in data.get("pipelines") or []:
        for s in p.get("stages") or []:
            out[s["id"]] = {"pipeline": p.get("name"),
                            "stage": s.get("name"),
                            "closed": bool(s.get("closedStage"))}
    return out


def _classify(deal, stages):
    st = stages.get(deal.get("stageId")) or {}
    name = (st.get("stage") or deal.get("stageName") or "").lower()
    if "archived" in name:
        return "archived"
    if st.get("closed") or name in ("sold", "closed", "leased"):
        return "closed"
    if "under contract" in name:
        return "under_contract"
    return "active_other"


def _appointments_funnel(days=90):
    """Set / met counts from the synced appointments table, by source
    bucket and by agent."""
    out = {"by_source": {}, "by_agent": {}, "set": 0, "met": 0}
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source, agent_name, status, outcome
                    FROM appointments
                    WHERE created_at >= NOW() - make_interval(days => %s)
                      AND COALESCE(status, '') != 'canceled'
                """, (int(days),))
                for source, agent, status, outcome in cur.fetchall():
                    b = bucket_for(source)
                    met = (outcome or "").lower() == "showed" \
                        or (status or "").lower() == "showed"
                    out["set"] += 1
                    out["met"] += 1 if met else 0
                    sb = out["by_source"].setdefault(b, {"set": 0, "met": 0})
                    sb["set"] += 1
                    sb["met"] += 1 if met else 0
                    if agent:
                        ab = out["by_agent"].setdefault(agent,
                                                        {"set": 0, "met": 0})
                        ab["set"] += 1
                        ab["met"] += 1 if met else 0
    except Exception as e:
        out["error"] = str(e)[:150]
    return out


def build_snapshot(client):
    """One pass over FUB deals + pipelines + the appointments table."""
    now = datetime.now(timezone.utc)
    stages = _stage_map(client)
    deals = _fetch_all_deals(client)

    person_source = {}

    def _deal_bucket(deal):
        """Source bucket via the linked person (fetch lazily), else None."""
        ppl = deal.get("people") or []
        pid = (ppl[0] or {}).get("id") if ppl and isinstance(ppl[0], dict) \
            else (ppl[0] if ppl else None)
        if not pid:
            return None
        if pid not in person_source:
            try:
                p = client._request("GET", "people/%s" % pid,
                                    params={"fields": "id,source"}) or {}
                person_source[pid] = (p.get("source") or "")
            except Exception:
                person_source[pid] = ""
        src = person_source[pid]
        return bucket_for(src) if src else None

    deals = [d for d in deals if not is_admin_deal(d)]
    coverage = {"deals_total": len(deals), "with_price": 0,
                "with_commission": 0, "with_person_link": 0}
    closed, under_contract = [], []
    for d in deals:
        kind = _classify(d, stages)
        if d.get("price"):
            coverage["with_price"] += 1
        if (d.get("commissionValue") or 0) > 0:
            coverage["with_commission"] += 1
        if d.get("people"):
            coverage["with_person_link"] += 1
        if kind == "closed":
            closed.append(d)
        elif kind == "under_contract":
            under_contract.append(d)

    def _row(d):
        # FUB's commissionValue arrives two ways in this book (probe Nov
        # 4 2026: "3" on a $725k deal): a small number is a PERCENT of
        # price, a large one is dollars. <= 10 with a price = percent.
        cv = float(d.get("commissionValue") or 0)
        price_v = float(d.get("price") or 0)
        if 0 < cv <= 10 and price_v:
            gci = round(price_v * cv / 100.0, 2)
        else:
            gci = cv or None
        bucket = _deal_bucket(d)
        fee = REFERRAL_FEES.get(bucket or "", 0.0)
        return {
            "name": (d.get("name") or "")[:80],
            "price": float(d.get("price") or 0) or None,
            "gci": gci,
            "bucket": bucket,
            "referral_fee_pct": int(fee * 100) if bucket else None,
            "net_gci": round(gci * (1 - fee), 2)
                       if (gci and bucket is not None) else None,
            "projected_close": (d.get("projectedCloseDate") or "")[:10]
                               or None,
            "entered_stage": (d.get("enteredStageAt") or "")[:10] or None,
        }

    closed_rows = [_row(d) for d in closed]
    uc_rows = [_row(d) for d in under_contract]

    cutoff_90 = (now - timedelta(days=90)).date().isoformat()
    closed_90 = [r for r in closed_rows
                 if (r["entered_stage"] or "") >= cutoff_90]

    def _window(rows, days):
        hi = (now + timedelta(days=days)).date().isoformat()
        lo = now.date().isoformat()
        w = [r for r in rows if r["projected_close"]
             and lo <= r["projected_close"] <= hi]
        return {
            "deals": len(w),
            "gross_volume": round(sum(r["price"] or 0 for r in w), 2),
            "gci_known": round(sum(r["gci"] or 0 for r in w), 2),
            "net_gci_known": round(sum(r["net_gci"] or 0 for r in w), 2),
        }

    by_source = {}
    for r in closed_90:
        b = r["bucket"] or "unlinked"
        sb = by_source.setdefault(b, {"closings": 0, "volume": 0.0,
                                      "gci": 0.0, "net_gci": 0.0})
        sb["closings"] += 1
        sb["volume"] = round(sb["volume"] + (r["price"] or 0), 2)
        sb["gci"] = round(sb["gci"] + (r["gci"] or 0), 2)
        sb["net_gci"] = round(sb["net_gci"] + (r["net_gci"] or 0), 2)

    return {
        "as_of": now.isoformat(),
        "coverage": coverage,
        "referral_fees": {k: int(v * 100) for k, v in REFERRAL_FEES.items()},
        "funnel_90d": {
            "appointments": _appointments_funnel(90),
            "under_contract_now": len(uc_rows),
            "closed_90d": len(closed_90),
        },
        "closed_by_source_90d": by_source,
        "pipeline": {
            "under_contract": uc_rows,
            "next_30d": _window(uc_rows, 30),
            "next_60d": _window(uc_rows, 60),
            "next_90d": _window(uc_rows, 90),
        },
        "closed_90d_rows": closed_90,
        "notes": [
            "Every figure comes from FUB deals/pipelines and synced "
            "appointments. Nothing is estimated. One format rule: a "
            "commissionValue of 10 or less on a priced deal is read as "
            "a PERCENT of price (how this book enters it), larger "
            "values as dollars.",
            "net_gci only exists where FUB carries BOTH a commission "
            "value and a person link (for the source bucket). Coverage "
            "says how much of the book qualifies; the upstream fix is "
            "deal hygiene in Dotloop/FUB (price, commission, person on "
            "every deal).",
            "Referral fees: Ylopo 40% to LPT, probate 25%, zBuyer and "
            "Realtor.com 0% (Realtor.com is a per-lead cost, see the "
            "Friday ladder receipt).",
        ],
    }


def get_snapshot(client, force=False):
    """Cached snapshot (30 min TTL): the pulse feed and the /revenue page
    both read this without hammering FUB."""
    with _LOCK:
        if (not force and _CACHE["data"]
                and time.time() - _CACHE["at"] < _CACHE_TTL):
            return _CACHE["data"]
    data = build_snapshot(client)
    with _LOCK:
        _CACHE["at"] = time.time()
        _CACHE["data"] = data
    return data
