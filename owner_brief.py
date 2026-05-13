"""
owner_brief.py — aggregates the full business state into one JSON object.

Called by GET /api/owner/daily-brief and the 7am owner email scheduler.
Perplexity consumes this endpoint to act as a virtual team owner.

JSON contract version: 1.0
See /api/owner/contract for the annotated schema.
"""

import os
import logging
from datetime import datetime, timezone, timedelta, date

logger = logging.getLogger(__name__)


def _et_date(dt_utc) -> str:
    """Convert a UTC datetime to YYYY-MM-DD in America/New_York. Safe fallback."""
    try:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo  # type: ignore
        return dt_utc.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return dt_utc.strftime("%Y-%m-%d")


def _jsonify(obj):
    """
    Recursively convert any non-JSON-serializable values (datetime, Decimal, date)
    in a dict/list structure to JSON-safe types. Returns a new object.
    """
    from datetime import datetime as _dt, date as _date
    from decimal import Decimal
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, _dt):
        return obj.isoformat()
    if isinstance(obj, _date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


# ── Source normalisation map ──────────────────────────────────────────────────
# Groups raw FUB source strings into owner-friendly buckets.
_SOURCE_BUCKETS = {
    "ylopo":        ["Ylopo", "Ylopo PPC+", "Ylopo GBP Ads", "Ylopo Seller",
                     "Ylopo Prospecting"],
    "zbuyer":       ["Zbuyer", "zbuyer.com", "Z-Buyer"],
    "isa":          ["ISA", "ISA Referral", "Fhalen"],
    "referral":     ["Referral", "Sphere", "Past Client", "Past client"],
    "ppc":          ["Google PPC", "PPC", "Paid Search", "Google Ads"],
    "social":       ["Facebook", "Instagram", "Social Media", "Meta"],
    "batchleads":   ["BatchLeads", "batch_leads", "Batch Leads"],
    "other":        [],   # catch-all
}

def _bucket_source(raw_source: str) -> str:
    s = (raw_source or "").strip()
    for bucket, aliases in _SOURCE_BUCKETS.items():
        if s in aliases or s.lower() == bucket:
            return bucket
    if "ylopo" in s.lower():
        return "ylopo"
    if "zbuyer" in s.lower() or "z-buyer" in s.lower():
        return "zbuyer"
    if "referral" in s.lower() or "sphere" in s.lower():
        return "referral"
    return "other"


# ── Lead gen section ──────────────────────────────────────────────────────────

def _build_lead_gen(fub_client, hours_24=True):
    """
    Pull new lead counts by source from FUB for the last 24h and 7d.
    Uses FUB /v1/people filtered by createdAt. Falls back gracefully.
    """
    result = {
        "last_24h": {"total": 0, "by_source": {}},
        "last_7d":  {"total": 0, "by_source": {}},
        "speed_to_lead": {
            "enabled": False,
            "note": "ENABLE_SPEED_TO_LEAD=False in config — re-enable for live tracking",
        },
        "pct_reached": {
            "last_7d": None,
            "note": "Calculated from FUB call logs vs new lead count",
        },
        "data_quality": "live",
    }
    try:
        import config as _cfg
        result["speed_to_lead"]["enabled"] = bool(
            getattr(_cfg, "ENABLE_SPEED_TO_LEAD", False)
        )

        now = datetime.now(timezone.utc)
        cutoff_24h = now - timedelta(hours=24)
        cutoff_7d  = now - timedelta(days=7)

        # FUB people sorted by created (desc) — only pull recent ones
        people_7d = fub_client.get_people_since(cutoff_7d, limit=500)

        buckets_24h: dict = {}
        buckets_7d:  dict = {}

        for p in people_7d:
            created_str = p.get("created") or p.get("createdAt") or ""
            try:
                created_dt = datetime.fromisoformat(
                    created_str.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                continue

            bucket = _bucket_source(p.get("source", ""))
            buckets_7d[bucket] = buckets_7d.get(bucket, 0) + 1

            if created_dt >= cutoff_24h:
                buckets_24h[bucket] = buckets_24h.get(bucket, 0) + 1

        result["last_24h"]["total"]     = sum(buckets_24h.values())
        result["last_24h"]["by_source"] = dict(sorted(buckets_24h.items(), key=lambda x: -x[1]))
        result["last_7d"]["total"]      = sum(buckets_7d.values())
        result["last_7d"]["by_source"]  = dict(sorted(buckets_7d.items(), key=lambda x: -x[1]))

    except Exception as e:
        logger.warning("owner_brief lead_gen section failed: %s", e)
        result["data_quality"] = f"partial — {e}"

    return result


# ── Conversion section ────────────────────────────────────────────────────────

def _build_conversion(fub_client, db):
    """
    Appointment set/show metrics from DB + deal closings from deal_log.
    """
    import config as _cfg
    now = datetime.now(timezone.utc)

    # Appointment stats from our table
    apt_stats = db.get_appointment_stats(days=30)

    # Upcoming appointments (next 14 days from FUB — live)
    upcoming = []
    try:
        since_now  = now
        until_14d  = now + timedelta(days=14)
        raw_appts  = fub_client.get_appointments(since=since_now, until=until_14d)
        for a in raw_appts[:20]:  # cap for JSON size
            invitees = a.get("invitees", [])
            lead_name  = next((i.get("name") for i in invitees if i.get("personId")), "Unknown")
            agent_name = next((i.get("name") for i in invitees if i.get("userId") and not i.get("personId")), "?")
            upcoming.append({
                "fub_appt_id": a.get("id"),
                "lead_name":   lead_name,
                "agent_name":  agent_name,
                "start":       a.get("start"),
                "title":       a.get("title", ""),
            })
    except Exception as e:
        logger.warning("owner_brief upcoming appts failed: %s", e)

    # Deal closings + active contracts from deal_log
    closings_mtd = []
    closings_ytd_count = 0
    gci_mtd = 0.0
    gci_ytd = 0.0
    active_contracts = []
    pipeline_gci = 0.0
    try:
        today = now.date()
        month_start = today.replace(day=1)
        year_start  = today.replace(month=1, day=1)
        year_end    = today.replace(month=12, day=31)

        def _gci(d):
            return float(d.get("gci") or d.get("gci_estimated") or 0)

        # Closed deals MTD
        mtd_deals = db.get_deals_in_range(str(month_start), str(today), stage="closing")
        for d in mtd_deals:
            closings_mtd.append({
                "agent":      d.get("agent_name"),
                "close_date": str(d.get("close_date", "")),
                "gci":        _gci(d),
                "sale_price": float(d.get("sale_price") or 0),
            })
            gci_mtd += _gci(d)

        # Closed deals YTD
        ytd_deals = db.get_deals_in_range(str(year_start), str(today), stage="closing")
        closings_ytd_count = len(ytd_deals)
        gci_ytd = sum(_gci(d) for d in ytd_deals)

        # Active contracts in pipeline (opened this year)
        contracts = db.get_deals_in_range(str(year_start), str(year_end), stage="contract")
        for d in contracts:
            active_contracts.append({
                "agent":         d.get("agent_name"),
                "contract_date": str(d.get("contract_date", "")),
                "sale_price":    float(d.get("sale_price") or 0),
                "gci":           _gci(d),
            })
            pipeline_gci += _gci(d)

    except Exception as e:
        logger.warning("owner_brief conversion deals section failed: %s", e)

    # Pond email/SMS conversion stats (last 7d)
    pond_7d = {}
    try:
        pond_7d = db.get_pond_brief_stats_7d()
    except Exception as e:
        logger.warning("owner_brief pond_7d failed: %s", e)

    return {
        "appointments": apt_stats,
        "upcoming_appointments": upcoming,
        "ai_outreach_conversions_7d": pond_7d,
        "closings": {
            "mtd_count":       len(closings_mtd),
            "mtd_gci":         round(gci_mtd, 2),
            "ytd_count":       closings_ytd_count,
            "ytd_gci":         round(gci_ytd, 2),
            "mtd_deals":       closings_mtd,
            "note":            "GCI estimated from sale_price x agent commission_pct in goals. 0 = agent has no goal set or FUB deal has no price.",
        },
        "pipeline_contracts": {
            "active_count":  len(active_contracts),
            "pipeline_gci":  round(pipeline_gci, 2),
            "contracts":     active_contracts[:20],
            "note":          "Contracts not yet closed — projected GCI in pipeline.",
        },
    }


# ── Pipeline at-risk section ──────────────────────────────────────────────────

def _build_pipeline_risks(fub_client, db):
    """
    Surfaces leads that need human attention:
      - ISA handoffs with no agent action (>1h)
      - Appointments past due with no outcome
      - OOC leads per agent (MAV_NUDGE_OUTSTANDING)
    """
    risks = {
        "isa_handoffs_no_action": [],
        "apt_outcome_overdue":    [],
        "ooc_by_agent":           [],
        "data_quality":           "live",
    }

    # Overdue appointment outcomes (from our table)
    try:
        overdue = db.get_overdue_appointments(hours_past=4)
        risks["apt_outcome_overdue"] = [
            {
                "fub_appt_id":  r["fub_appt_id"],
                "person_id":    r["person_id"],
                "person_name":  r["person_name"],
                "agent_name":   r["agent_name"],
                "start_time":   r["start_time"].isoformat() if r["start_time"] else None,
                "hours_past":   round(
                    (datetime.now(timezone.utc) - r["start_time"]).total_seconds() / 3600, 1
                ) if r["start_time"] else None,
                "fub_url": f"https://app.followupboss.com/2/people/detail/{r['person_id']}",
            }
            for r in overdue
        ]
    except Exception as e:
        logger.warning("owner_brief apt overdue failed: %s", e)
        risks["data_quality"] = f"partial — {e}"

    # ISA transfers with no agent action — query isa_transfers table
    try:
        transfers = db.get_isa_transfers_pending_action(hours=2)
        risks["isa_handoffs_no_action"] = transfers
    except Exception as e:
        logger.warning("owner_brief isa_transfers failed: %s", e)

    return risks


# ── Manager / ISA SLA section ─────────────────────────────────────────────────

def _build_manager_sla(fub_client, db):
    """
    Joe's coaching SLA and Fhalen's ISA performance from cached FUB data.
    Pulls from existing /api/manager and /api/isa data shapes where possible.
    """
    result = {
        "joe": {
            "dropped_ball_leads": 0,
            "dropped_ball_list":  [],
            "coaching_overdue":   0,
            "data_quality":       "live",
        },
        "isa": {
            "name": "Fhalen Tendencia",
            "contacts_7d":         0,
            "transfers_7d":        0,
            "pending_handoffs":    0,
            "stale_leads":         0,
            "contact_rate_pct":    None,
            "data_quality":        "live",
        },
    }

    import config as _cfg

    # Dropped-ball leads: Fhalen_Pending tag with no update in STALE_LEAD_DAYS days
    try:
        stale_days = getattr(_cfg, "STALE_LEAD_DAYS", 5)
        pending_tag = getattr(_cfg, "DROPPED_BALL_TAG", "Fhalen_Pending")
        people_pending = fub_client.search_people_by_tag(pending_tag, limit=50)
        dropped = []
        now = datetime.now(timezone.utc)
        for p in people_pending:
            last_activity_str = (
                p.get("lastActivityAt") or p.get("updated") or ""
            )
            try:
                last_dt = datetime.fromisoformat(
                    last_activity_str.replace("Z", "+00:00")
                )
                days_stale = (now - last_dt).days
            except Exception:
                days_stale = 0
            if days_stale >= stale_days:
                dropped.append({
                    "person_id":   p.get("id"),
                    "name":        p.get("name", "Unknown"),
                    "days_stale":  days_stale,
                    "assigned_agent": (p.get("assignedTo") or {}).get("name"),
                    "fub_url": f"https://app.followupboss.com/2/people/detail/{p.get('id')}",
                })
        result["joe"]["dropped_ball_leads"] = len(dropped)
        result["joe"]["dropped_ball_list"]  = dropped[:15]  # cap for JSON size
    except Exception as e:
        logger.warning("owner_brief dropped_ball failed: %s", e)
        result["joe"]["data_quality"] = f"partial — {e}"

    # ISA transfers from DB
    try:
        isa_transfers = db.get_isa_transfer_stats_7d()
        result["isa"].update(isa_transfers)
    except Exception as e:
        logger.warning("owner_brief ISA transfer stats failed: %s", e)

    return result


# ── Agent accountability section ─────────────────────────────────────────────

def _build_agent_accountability(db):
    """
    Per-agent GCI pace vs goal and call activity from cached YTD data.
    Uses agent_ytd_cache (updated by goals_sync job Mon/Thu 6am ET) and
    agent goals from the goals table.

    Returns a list of agent dicts plus team-level GCI totals.
    """
    result = {
        "data_quality": "live",
        "ytd_team_gci": 0.0,
        "ytd_team_closings": 0,
        "team_gci_goal": 0.0,
        "team_gci_pace_pct": None,
        "agents": [],
        "below_call_pace": [],   # agents averaging < 25 calls/week
        "behind_gci_goal": [],   # agents at < 70% of prorated GCI goal
        "cache_note": None,
    }

    try:
        now = datetime.now(timezone.utc)
        year = now.year

        # Weeks elapsed since Jan 1 (minimum 1 to avoid div/0)
        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        weeks_elapsed = max((now - jan1).days / 7, 1)

        # Load goals and YTD actuals — all in 3 DB round-trips (not N per agent)
        all_goals   = db.get_all_goals(year=year)         # list of goal dicts
        ytd_cache   = db.get_ytd_cache(year=year)         # {name: {calls_ytd, appts_ytd, ...}}
        gci_ytd_map = db.get_all_agent_gci_ytd(year=year) # {name: {gci_ytd, closings_ytd}}
        cache_ts    = db.get_cache_updated_at(year=year)
        result["cache_note"] = f"ytd_cache updated {cache_ts}" if cache_ts else "ytd_cache empty — goals_sync may not have run yet"

        goal_map = {g["agent_name"]: g for g in all_goals}

        team_gci_goal = 0.0
        team_ytd_gci  = 0.0
        team_ytd_closings = 0

        for agent_name, ytd in ytd_cache.items():
            goal = goal_map.get(agent_name, {})
            gci_goal = float(goal.get("gci_goal") or 0)
            team_gci_goal += gci_goal

            # GCI actuals from batch deal_log query (one query for all agents)
            gci_data     = gci_ytd_map.get(agent_name, {})
            gci_ytd      = float(gci_data.get("gci_ytd") or 0)
            closings_ytd = int(gci_data.get("closings_ytd") or 0)
            team_ytd_gci      += gci_ytd
            team_ytd_closings += closings_ytd

            calls_ytd  = int(ytd.get("calls_ytd") or 0)
            appts_ytd  = int(ytd.get("appts_ytd") or 0)
            calls_per_week = round(calls_ytd / weeks_elapsed, 1)

            # GCI pace: annualize the YTD GCI rate
            year_fraction = weeks_elapsed / 52
            gci_pace_annualized = round(gci_ytd / year_fraction, 2) if year_fraction > 0 and gci_ytd > 0 else 0.0

            # Prorated goal = gci_goal * (weeks_elapsed / 52)
            prorated_goal = round(gci_goal * year_fraction, 2)
            gci_pace_pct  = round(gci_ytd / prorated_goal * 100, 1) if prorated_goal > 0 else None

            status = "no_goal"
            if gci_goal > 0 and gci_pace_pct is not None:
                if gci_pace_pct >= 90:
                    status = "on_pace"
                elif gci_pace_pct >= 70:
                    status = "slightly_behind"
                else:
                    status = "behind"

            agent_row = {
                "agent_name":           agent_name,
                "gci_goal":             gci_goal,
                "gci_ytd":              gci_ytd,
                "closings_ytd":         closings_ytd,
                "gci_pace_annualized":  gci_pace_annualized,
                "gci_pace_pct":         gci_pace_pct,
                "prorated_goal":        prorated_goal,
                "status":               status,
                "calls_ytd":            calls_ytd,
                "calls_per_week":       calls_per_week,
                "appts_ytd":            appts_ytd,
            }
            result["agents"].append(agent_row)

            # Flag lists for quick Perplexity access
            if calls_ytd > 0 and calls_per_week < 25:
                result["below_call_pace"].append({
                    "agent_name":     agent_name,
                    "calls_per_week": calls_per_week,
                    "calls_ytd":      calls_ytd,
                })
            if status == "behind":
                result["behind_gci_goal"].append({
                    "agent_name":    agent_name,
                    "gci_pace_pct":  gci_pace_pct,
                    "gci_ytd":       gci_ytd,
                    "prorated_goal": prorated_goal,
                })

        result["agents"].sort(key=lambda x: x.get("gci_ytd", 0), reverse=True)
        result["ytd_team_gci"]       = round(team_ytd_gci, 2)
        result["ytd_team_closings"]  = team_ytd_closings
        result["team_gci_goal"]      = round(team_gci_goal, 2)

        if team_gci_goal > 0:
            year_fraction = weeks_elapsed / 52
            prorated_team = team_gci_goal * year_fraction
            result["team_gci_pace_pct"] = round(team_ytd_gci / prorated_team * 100, 1) if prorated_team > 0 else None

    except Exception as e:
        logger.warning("_build_agent_accountability failed: %s", e)
        result["data_quality"] = f"error — {e}"

    return result


# ── Tech health section ───────────────────────────────────────────────────────

def _build_tech_health(db):
    """Aggregate automation_event_log for system health."""
    health = db.get_tech_health_summary(hours=24)

    # Pond caps from env
    import config as _cfg
    try:
        pb_cap    = int(os.environ.get("PB_DAILY_CAP", "25"))
        hg_cap    = getattr(_cfg, "HEYGEN_DAILY_CAP", 12)
        pb_used   = db.count_pond_sms_today()
        hg_used   = db.count_heygen_today()
    except Exception:
        pb_cap = hg_cap = pb_used = hg_used = None

    health["pb_daily_cap"]   = pb_cap
    health["pb_used_today"]  = pb_used
    health["pb_remaining"]   = (pb_cap - pb_used) if (pb_cap and pb_used is not None) else None
    health["hg_daily_cap"]   = hg_cap
    health["hg_used_today"]  = hg_used
    health["hg_remaining"]   = (hg_cap - hg_used) if (hg_cap and hg_used is not None) else None

    # Scheduler job statuses are in-memory in app.py — can't query from here.
    # The /api/health endpoint exposes them; we reference that in the contract doc.
    health["scheduler_status_url"] = "/api/health"
    return health


# ── Top-3 AI recommendations ──────────────────────────────────────────────────

def _build_recommendations(lead_gen, conversion, pipeline, manager, tech,
                           accountability=None):
    """
    Generate up to 3 plain-English action items for Barry based on the data.
    Deterministic rule-based — no Claude call here (keep this endpoint fast).
    """
    actions = []

    # No-show / overdue appointments
    overdue_apts = len(pipeline.get("apt_outcome_overdue", []))
    if overdue_apts > 0:
        actions.append({
            "priority": 1,
            "category": "appointments",
            "action": (
                f"{overdue_apts} appointment(s) passed with no outcome logged. "
                f"Text Joe now: who showed, who didn't. "
                f"No-show recovery should fire within 4 hours of the appointment."
            ),
        })

    # ISA handoffs with no agent action
    isa_no_action = len(pipeline.get("isa_handoffs_no_action", []))
    if isa_no_action > 0:
        actions.append({
            "priority": 2 if not actions else len(actions) + 1,
            "category": "isa_sla",
            "action": (
                f"{isa_no_action} ISA handoff(s) have no agent call logged in 2+ hours. "
                f"Text Joe immediately — these are warm live transfers going cold."
            ),
        })

    # Dropped-ball leads
    dropped = manager.get("joe", {}).get("dropped_ball_leads", 0)
    if dropped > 0:
        actions.append({
            "priority": len(actions) + 1,
            "category": "manager_sla",
            "action": (
                f"{dropped} lead(s) with Fhalen_Pending tag have gone {dropped} days with no agent update. "
                f"Joe should clear these in today's session — add to the Monday KPI debrief."
            ),
        })

    # Tech errors
    errors = tech.get("errors", 0)
    if errors > 5:
        actions.append({
            "priority": len(actions) + 1,
            "category": "tech_health",
            "action": (
                f"{errors} automation error(s) in the last 24 hours. "
                f"Check /api/owner/tech-issues for specifics before the next mailer run."
            ),
        })

    # Low AI outreach
    sms_24h = tech.get("sms_sent", 0)
    if sms_24h == 0:
        actions.append({
            "priority": len(actions) + 1,
            "category": "ai_outreach",
            "action": (
                "No SMS sent in the last 24 hours. "
                "Check PB_DAILY_CAP, Project Blue API key, and the pond mailer scheduler."
            ),
        })

    # Lead flow low
    leads_24h = lead_gen.get("last_24h", {}).get("total", 0)
    if leads_24h < 3:
        actions.append({
            "priority": len(actions) + 1,
            "category": "lead_gen",
            "action": (
                f"Only {leads_24h} new leads in the last 24 hours — below normal flow. "
                f"Check Ylopo dashboard and BatchLeads for any paused campaigns."
            ),
        })

    # Agents behind GCI goal
    if accountability:
        behind = accountability.get("behind_gci_goal", [])
        if behind:
            names = ", ".join(a["agent_name"].split()[0] for a in behind[:3])
            actions.append({
                "priority": len(actions) + 1,
                "category": "agent_accountability",
                "action": (
                    f"{len(behind)} agent(s) below 70% of prorated GCI goal: {names}. "
                    f"Pull their call pace and appointment set rate in today's 1-on-1."
                ),
            })

    # Cap the list at 3 most important
    return sorted(actions, key=lambda x: x["priority"])[:3]


# ── Main build function ───────────────────────────────────────────────────────

def build_owner_daily_brief(fub_client=None, db=None):
    """
    Build and return the full owner_daily_brief dict.
    Called by GET /api/owner/daily-brief (cached 10 min).

    If fub_client/db are not provided, imports them internally.
    """
    if fub_client is None:
        from fub_client import FUBClient
        fub_client = FUBClient()
    if db is None:
        import db as _db
        db = _db

    now = datetime.now(timezone.utc)

    lead_gen      = _build_lead_gen(fub_client)
    conversion    = _build_conversion(fub_client, db)
    pipeline      = _build_pipeline_risks(fub_client, db)
    manager       = _build_manager_sla(fub_client, db)
    accountability = _build_agent_accountability(db)
    tech          = _build_tech_health(db)
    actions       = _build_recommendations(lead_gen, conversion, pipeline, manager, tech,
                                           accountability=accountability)

    raw = {
        "schema_version":    "1.0",
        "generated_at":      now.isoformat(),
        "business_date":     _et_date(now),
        "team":              "Legacy Home Team",
        "lead_gen":          lead_gen,
        "conversion":        conversion,
        "pipeline_risks":    pipeline,
        "manager_sla":       manager,
        "agent_accountability": accountability,
        "tech_health":       tech,
        "top_3_actions":     actions,
        "reference_urls": {
            "full_dashboard":    "/",
            "pond_admin":        "/pond-admin",
            "leadstream":        "/leadstream",
            "goals":             "/goals",
            "tech_issues":       "/api/owner/tech-issues",
            "lead_issues":       "/api/owner/lead-issues",
            "scheduler_health":  "/api/health",
        },
    }
    return _jsonify(raw)


# ── Lead-issues detail list ───────────────────────────────────────────────────

def build_lead_issues(fub_client=None, db=None):
    """
    Returns a flat list of specific leads needing human attention.
    Each item includes person_id, name, issue_type, severity, and fub_url.
    Called by GET /api/owner/lead-issues.
    """
    if fub_client is None:
        from fub_client import FUBClient
        fub_client = FUBClient()
    if db is None:
        import db as _db
        db = _db

    issues = []

    # 1. Overdue appointment outcomes
    try:
        for r in db.get_overdue_appointments(hours_past=4):
            hours_past = round(
                (datetime.now(timezone.utc) - r["start_time"]).total_seconds() / 3600, 1
            ) if r["start_time"] else None
            issues.append({
                "issue_type":  "apt_outcome_overdue",
                "severity":    "high" if (hours_past or 0) > 8 else "medium",
                "person_id":   r["person_id"],
                "person_name": r["person_name"],
                "agent_name":  r["agent_name"],
                "detail":      f"Appointment was {hours_past}h ago with no outcome logged",
                "start_time":  r["start_time"].isoformat() if r["start_time"] else None,
                "fub_url": f"https://app.followupboss.com/2/people/detail/{r['person_id']}",
            })
    except Exception as e:
        logger.warning("build_lead_issues apt section: %s", e)

    # 2. ISA handoffs with no agent action
    try:
        for t in db.get_isa_transfers_pending_action(hours=2):
            issues.append({
                "issue_type":  "isa_handoff_no_action",
                "severity":    "high",
                "person_id":   t.get("person_id"),
                "person_name": t.get("person_name"),
                "agent_name":  t.get("agent_name"),
                "detail":      f"ISA transfer {t.get('hours_since', '?')}h ago — no agent call logged",
                "fub_url": (
                    f"https://app.followupboss.com/2/people/detail/{t.get('person_id')}"
                    if t.get("person_id") else None
                ),
            })
    except Exception as e:
        logger.warning("build_lead_issues ISA section: %s", e)

    # 3. Dropped-ball leads (Fhalen_Pending, stale)
    try:
        import config as _cfg
        stale_days = getattr(_cfg, "STALE_LEAD_DAYS", 5)
        pending_tag = getattr(_cfg, "DROPPED_BALL_TAG", "Fhalen_Pending")
        people = fub_client.search_people_by_tag(pending_tag, limit=30)
        now = datetime.now(timezone.utc)
        for p in people:
            last_str = p.get("lastActivityAt") or p.get("updated") or ""
            try:
                last_dt   = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
                days_stale = (now - last_dt).days
            except Exception:
                days_stale = 0
            if days_stale >= stale_days:
                issues.append({
                    "issue_type":  "dropped_ball",
                    "severity":    "high" if days_stale > 7 else "medium",
                    "person_id":   p.get("id"),
                    "person_name": p.get("name"),
                    "agent_name":  (p.get("assignedTo") or {}).get("name"),
                    "detail":      f"{days_stale}d stale with Fhalen_Pending tag — Joe should follow up",
                    "fub_url": f"https://app.followupboss.com/2/people/detail/{p.get('id')}",
                })
    except Exception as e:
        logger.warning("build_lead_issues dropped_ball section: %s", e)

    # Sort by severity
    sev_order = {"high": 0, "medium": 1, "low": 2}
    issues.sort(key=lambda x: sev_order.get(x.get("severity", "low"), 2))

    return _jsonify({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_issues": len(issues),
        "high":   sum(1 for i in issues if i.get("severity") == "high"),
        "medium": sum(1 for i in issues if i.get("severity") == "medium"),
        "issues": issues,
    })


# ── Tech-issues detail list ───────────────────────────────────────────────────

def build_tech_issues(db=None):
    """
    Returns a flat list of automation errors and system warnings.
    Called by GET /api/owner/tech-issues.
    """
    if db is None:
        import db as _db
        db = _db

    errors = db.get_automation_events(hours=24, success=False, limit=100)

    issues = []
    for e in errors:
        issues.append({
            "event_type":    e["event_type"],
            "person_id":     e["person_id"],
            "person_name":   e["person_name"],
            "triggered_by":  e["triggered_by"],
            "error_message": e["error_message"],
            "created_at":    e["created_at"].isoformat() if e["created_at"] else None,
        })

    # Cap data warnings
    warnings = []
    try:
        import config as _cfg
        pb_cap  = int(os.environ.get("PB_DAILY_CAP", "25"))
        pb_used = db.count_pond_sms_today()
        if pb_used and pb_used >= pb_cap:
            warnings.append({
                "type": "cap_reached",
                "system": "Project Blue SMS",
                "detail": f"Daily cap of {pb_cap} reached — no more SMS today",
                "severity": "medium",
            })
        hg_cap  = getattr(_cfg, "HEYGEN_DAILY_CAP", 12)
        hg_used = db.count_heygen_today()
        if hg_used and hg_used >= hg_cap:
            warnings.append({
                "type": "cap_reached",
                "system": "HeyGen Video",
                "detail": f"Daily cap of {hg_cap} reached — leads getting text-only Email 1",
                "severity": "medium",
            })
    except Exception as ex:
        logger.warning("build_tech_issues cap check: %s", ex)

    return _jsonify({
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "window_hours":  24,
        "total_errors":  len(errors),
        "warnings":      warnings,
        "errors":        issues,
    })
