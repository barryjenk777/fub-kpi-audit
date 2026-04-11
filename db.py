"""
Database layer for Legacy Home Team KPI Audit.

Provides Postgres persistence (via DATABASE_URL) with graceful fallback when
no database is configured. All public functions are safe to call — they
silently return None / empty structures if the DB is unavailable.

Tables
------
engagement_runs     — one row per agent per scoring run (replaces engagement_log.json)
leadstream_manifest — single-row JSONB store for the scoring manifest
agent_profiles      — FUB user profiles (name, email, user_id)
goal_tokens         — secure per-agent links for the self-service setup form
goals               — annual business goals per agent with SOI + conversion rates
deal_log            — contracts and closings synced from FUB Deals (via Dotloop)
"""

import os
import json
import logging
import secrets
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_db_url = None


def _get_url():
    global _db_url
    if _db_url is None:
        _db_url = os.environ.get("DATABASE_URL", "")
        if _db_url.startswith("postgres://"):
            _db_url = "postgresql://" + _db_url[len("postgres://"):]
    return _db_url


def is_available():
    return bool(_get_url())


@contextmanager
def get_conn():
    import psycopg2
    conn = psycopg2.connect(_get_url())
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS engagement_runs (
    id           SERIAL PRIMARY KEY,
    run_time     TIMESTAMPTZ NOT NULL,
    day_et       DATE        NOT NULL,
    mode         TEXT        NOT NULL DEFAULT 'full',
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_name   TEXT        NOT NULL,
    is_pond      BOOLEAN     NOT NULL DEFAULT FALSE,
    tagged       INTEGER     NOT NULL DEFAULT 0,
    actioned     INTEGER     NOT NULL DEFAULT 0,
    UNIQUE (run_time, agent_name, is_pond)
);
CREATE INDEX IF NOT EXISTS idx_er_day_et  ON engagement_runs (day_et DESC);
CREATE INDEX IF NOT EXISTS idx_er_agent   ON engagement_runs (agent_name, day_et DESC);

CREATE TABLE IF NOT EXISTS leadstream_manifest (
    id         SERIAL PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data       JSONB       NOT NULL
);

-- Agent roster pulled from FUB
CREATE TABLE IF NOT EXISTS agent_profiles (
    agent_name   TEXT PRIMARY KEY,
    fub_user_id  INTEGER,
    email        TEXT,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Secure tokens for agent self-service goal setup links
CREATE TABLE IF NOT EXISTS goal_tokens (
    id          SERIAL PRIMARY KEY,
    token       TEXT        NOT NULL UNIQUE DEFAULT gen_random_uuid()::text,
    agent_name  TEXT        NOT NULL REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '90 days',
    used_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_gt_token ON goal_tokens (token);

-- Annual business goals per agent
CREATE TABLE IF NOT EXISTS goals (
    id                      SERIAL PRIMARY KEY,
    agent_name              TEXT    NOT NULL REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    year                    INTEGER NOT NULL DEFAULT EXTRACT(YEAR FROM NOW()),

    -- Income goal
    gci_goal                NUMERIC(12,2) NOT NULL DEFAULT 0,

    -- Deal economics (what they net per closing after split)
    avg_sale_price          NUMERIC(12,2) NOT NULL DEFAULT 400000,
    commission_pct          NUMERIC(5,4)  NOT NULL DEFAULT 0.025,

    -- Sphere of influence (referrals / past clients — no prospecting needed)
    soi_closings_expected   INTEGER NOT NULL DEFAULT 0,
    soi_gci_expected        NUMERIC(12,2) NOT NULL DEFAULT 0,
    sphere_touch_monthly    INTEGER NOT NULL DEFAULT 2,

    -- Conversion rates (defaults = industry average for competent agent)
    call_to_appt_rate       NUMERIC(5,4)  NOT NULL DEFAULT 0.10,
    appt_to_contract_rate   NUMERIC(5,4)  NOT NULL DEFAULT 0.30,
    contract_to_close_rate  NUMERIC(5,4)  NOT NULL DEFAULT 0.80,

    -- Metadata
    set_by                  TEXT,   -- 'agent' or 'manager'
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (agent_name, year)
);

-- Contracts and closings synced from FUB Deals (populated by Dotloop via FUB)
CREATE TABLE IF NOT EXISTS deal_log (
    id              SERIAL PRIMARY KEY,
    fub_deal_id     INTEGER UNIQUE,
    agent_name      TEXT,
    deal_name       TEXT,           -- usually property address
    sale_price      NUMERIC(12,2),
    stage           TEXT,           -- 'contract' or 'closing'
    stage_raw       TEXT,           -- original FUB stage label
    contract_date   DATE,
    close_date      DATE,
    year            INTEGER,
    gci_estimated   NUMERIC(12,2),  -- sale_price * agent's commission_pct
    source          TEXT NOT NULL DEFAULT 'fub_sync',  -- 'fub_sync' or 'manual'
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dl_agent_year ON deal_log (agent_name, year);
CREATE INDEX IF NOT EXISTS idx_dl_stage      ON deal_log (stage, year);

-- Cached YTD actuals per agent (refreshed by scheduled job Mon/Thu 6am ET)
-- Avoids live FUB API calls on every scorecard page load
CREATE TABLE IF NOT EXISTS agent_ytd_cache (
    id           SERIAL PRIMARY KEY,
    agent_name   TEXT    NOT NULL,
    year         INTEGER NOT NULL,
    calls_ytd    INTEGER NOT NULL DEFAULT 0,
    appts_ytd    INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name, year)
);
"""


def init_db():
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Enable pgcrypto for gen_random_uuid() if not already enabled
                cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                cur.execute(SCHEMA_SQL)
        logger.info("Database schema verified / initialized.")
        return True
    except Exception as e:
        logger.warning("DB init failed (continuing without DB): %s", e)
        return False


# ---------------------------------------------------------------------------
# Engagement log helpers  (same as before)
# ---------------------------------------------------------------------------

def _et_offset():
    m = datetime.now(timezone.utc).month
    return -4 if 3 <= m <= 10 else -5


def write_engagement_entries(run_time_iso, mode, agents, pond):
    if not is_available():
        return False
    try:
        ET = timezone(timedelta(hours=_et_offset()))
        run_time = datetime.fromisoformat(run_time_iso.replace("Z", "+00:00"))
        day_et = run_time.astimezone(ET).date()
        rows = []
        for agent_name, counts in agents.items():
            rows.append((run_time, day_et, mode, agent_name, False,
                         counts.get("tagged", 0), counts.get("actioned", 0)))
        if pond:
            rows.append((run_time, day_et, mode, "__pond__", True,
                         pond.get("tagged", 0), pond.get("actioned", 0)))
        with get_conn() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute("""
                        INSERT INTO engagement_runs
                            (run_time, day_et, mode, agent_name, is_pond, tagged, actioned)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_time, agent_name, is_pond) DO UPDATE SET
                            tagged      = GREATEST(engagement_runs.tagged,   EXCLUDED.tagged),
                            actioned    = GREATEST(engagement_runs.actioned, EXCLUDED.actioned),
                            mode        = EXCLUDED.mode,
                            captured_at = NOW()
                    """, row)
        return True
    except Exception as e:
        logger.warning("write_engagement_entries failed: %s", e)
        return False


def read_engagement_log(days=7):
    if not is_available():
        return {}
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_time, mode, agent_name, is_pond,
                           MAX(tagged) AS tagged, MAX(actioned) AS actioned
                    FROM   engagement_runs
                    WHERE  run_time >= %s
                    GROUP  BY run_time, mode, agent_name, is_pond
                    ORDER  BY run_time
                """, (cutoff,))
                rows = cur.fetchall()
        result = {}
        for run_time, mode, agent_name, is_pond, tagged, actioned in rows:
            key = run_time.isoformat()
            if key not in result:
                result[key] = {"mode": mode, "agents": {}, "pond": {"tagged": 0, "actioned": 0}}
            if is_pond:
                result[key]["pond"] = {"tagged": tagged, "actioned": actioned}
            else:
                result[key]["agents"][agent_name] = {"tagged": tagged, "actioned": actioned}
        return result
    except Exception as e:
        logger.warning("read_engagement_log failed: %s", e)
        return {}


def write_engagement_from_log_dict(eng_log):
    if not is_available():
        return 0
    count = 0
    for run_time_iso, rec in eng_log.items():
        ok = write_engagement_entries(run_time_iso, rec.get("mode", "full"),
                                      rec.get("agents", {}), rec.get("pond", {}))
        if ok:
            count += 1
    return count


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def write_manifest(manifest):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM leadstream_manifest")
                cur.execute("INSERT INTO leadstream_manifest (data) VALUES (%s)",
                            (json.dumps(manifest),))
        return True
    except Exception as e:
        logger.warning("write_manifest failed: %s", e)
        return False


def read_manifest():
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM leadstream_manifest ORDER BY updated_at DESC LIMIT 1")
                row = cur.fetchone()
        if row:
            return row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return None
    except Exception as e:
        logger.warning("read_manifest failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Agent profiles
# ---------------------------------------------------------------------------

def upsert_agent_profile(agent_name, fub_user_id=None, email=None, is_active=True):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_profiles (agent_name, fub_user_id, email, is_active, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        fub_user_id = COALESCE(EXCLUDED.fub_user_id, agent_profiles.fub_user_id),
                        email       = COALESCE(EXCLUDED.email,       agent_profiles.email),
                        is_active   = EXCLUDED.is_active,
                        updated_at  = NOW()
                """, (agent_name, fub_user_id, email, is_active))
        return True
    except Exception as e:
        logger.warning("upsert_agent_profile failed: %s", e)
        return False


def get_agent_profiles(active_only=True):
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if active_only:
                    cur.execute("""
                        SELECT agent_name, fub_user_id, email, is_active
                        FROM   agent_profiles WHERE is_active = TRUE ORDER BY agent_name
                    """)
                else:
                    cur.execute("""
                        SELECT agent_name, fub_user_id, email, is_active
                        FROM   agent_profiles ORDER BY agent_name
                    """)
                rows = cur.fetchall()
        return [{"agent_name": r[0], "fub_user_id": r[1], "email": r[2], "is_active": r[3]}
                for r in rows]
    except Exception as e:
        logger.warning("get_agent_profiles failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Goal tokens
# ---------------------------------------------------------------------------

def create_goal_token(agent_name):
    """Generate a new setup link token for an agent. Returns the token string."""
    if not is_available():
        return None
    try:
        token = secrets.token_urlsafe(32)
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Expire any previous tokens for this agent
                cur.execute("""
                    UPDATE goal_tokens SET expires_at = NOW()
                    WHERE  agent_name = %s AND expires_at > NOW()
                """, (agent_name,))
                cur.execute("""
                    INSERT INTO goal_tokens (token, agent_name)
                    VALUES (%s, %s) RETURNING token
                """, (token, agent_name))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("create_goal_token failed: %s", e)
        return None


def resolve_goal_token(token):
    """
    Validate a token and return the agent_name, or None if invalid/expired.
    Does NOT mark it used — tokens are reusable so agents can update goals.
    """
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name FROM goal_tokens
                    WHERE  token = %s AND expires_at > NOW()
                """, (token,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("resolve_goal_token failed: %s", e)
        return None


def get_token_for_agent(agent_name):
    """Return the active token for an agent, or None."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT token FROM goal_tokens
                    WHERE  agent_name = %s AND expires_at > NOW()
                    ORDER  BY created_at DESC LIMIT 1
                """, (agent_name,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("get_token_for_agent failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------------

def _goal_row_to_dict(row, cols):
    d = dict(zip(cols, row))
    # Serialize dates and Decimals
    for k, v in d.items():
        if hasattr(v, 'isoformat'):
            d[k] = v.isoformat()
        elif hasattr(v, '__float__'):
            d[k] = float(v)
    return d


def upsert_goal(agent_name, year, gci_goal, avg_sale_price, commission_pct,
                soi_closings_expected=0, soi_gci_expected=0, sphere_touch_monthly=2,
                call_to_appt_rate=0.10, appt_to_contract_rate=0.30,
                contract_to_close_rate=0.80, set_by='manager', notes=None):
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO goals (
                        agent_name, year, gci_goal, avg_sale_price, commission_pct,
                        soi_closings_expected, soi_gci_expected, sphere_touch_monthly,
                        call_to_appt_rate, appt_to_contract_rate, contract_to_close_rate,
                        set_by, notes, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (agent_name, year) DO UPDATE SET
                        gci_goal               = EXCLUDED.gci_goal,
                        avg_sale_price         = EXCLUDED.avg_sale_price,
                        commission_pct         = EXCLUDED.commission_pct,
                        soi_closings_expected  = EXCLUDED.soi_closings_expected,
                        soi_gci_expected       = EXCLUDED.soi_gci_expected,
                        sphere_touch_monthly   = EXCLUDED.sphere_touch_monthly,
                        call_to_appt_rate      = EXCLUDED.call_to_appt_rate,
                        appt_to_contract_rate  = EXCLUDED.appt_to_contract_rate,
                        contract_to_close_rate = EXCLUDED.contract_to_close_rate,
                        set_by                 = EXCLUDED.set_by,
                        notes                  = EXCLUDED.notes,
                        updated_at             = NOW()
                    RETURNING *
                """, (agent_name, year, gci_goal, avg_sale_price, commission_pct,
                      soi_closings_expected, soi_gci_expected, sphere_touch_monthly,
                      call_to_appt_rate, appt_to_contract_rate, contract_to_close_rate,
                      set_by, notes))
                row = cur.fetchone()
                cols = [d[0] for d in cur.description]
        return _goal_row_to_dict(row, cols) if row else None
    except Exception as e:
        logger.warning("upsert_goal failed: %s", e)
        return None


def get_goal(agent_name, year=None):
    if not is_available():
        return None
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM goals WHERE agent_name=%s AND year=%s",
                            (agent_name, year))
                row = cur.fetchone()
                cols = [d[0] for d in cur.description] if cur.description else []
        return _goal_row_to_dict(row, cols) if row else None
    except Exception as e:
        logger.warning("get_goal failed: %s", e)
        return None


def get_all_goals(year=None):
    if not is_available():
        return []
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM goals WHERE year=%s ORDER BY agent_name", (year,))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
        return [_goal_row_to_dict(r, cols) for r in rows]
    except Exception as e:
        logger.warning("get_all_goals failed: %s", e)
        return []


def compute_targets(goal: dict) -> dict:
    """
    From a goal record, calculate the weekly activity targets an agent needs.
    Returns a dict of derived targets (not stored in DB).
    """
    gci          = float(goal.get("gci_goal", 0))
    avg_price    = float(goal.get("avg_sale_price", 400000))
    comm_pct     = float(goal.get("commission_pct", 0.025))
    soi_close    = int(goal.get("soi_closings_expected", 0))
    soi_gci      = float(goal.get("soi_gci_expected", 0))
    c2a          = float(goal.get("call_to_appt_rate", 0.10))
    a2c          = float(goal.get("appt_to_contract_rate", 0.30))
    c2cl         = float(goal.get("contract_to_close_rate", 0.80))

    avg_commission   = avg_price * comm_pct
    prospecting_gci  = max(gci - soi_gci, 0)
    closings_needed  = round(gci / avg_commission, 1) if avg_commission > 0 else 0
    prospect_close   = max(closings_needed - soi_close, 0)
    contracts_needed = round(prospect_close / c2cl, 1) if c2cl > 0 else 0
    appts_needed_yr  = round(contracts_needed / a2c, 1) if a2c > 0 else 0
    calls_needed_yr  = round(appts_needed_yr / c2a, 1) if c2a > 0 else 0
    appts_per_week   = round(appts_needed_yr / 50, 1)
    calls_per_week   = round(calls_needed_yr / 50, 1)

    return {
        "avg_commission":    round(avg_commission, 0),
        "closings_needed":   closings_needed,
        "soi_closings":      soi_close,
        "prospect_closings": prospect_close,
        "contracts_needed":  contracts_needed,
        "appts_needed_yr":   appts_needed_yr,
        "calls_needed_yr":   calls_needed_yr,
        "appts_per_week":    appts_per_week,
        "calls_per_week":    calls_per_week,
    }


# ---------------------------------------------------------------------------
# Deal log  (FUB Deals synced from Dotloop)
# ---------------------------------------------------------------------------

# Dotloop → FUB stage labels that indicate a signed contract
CONTRACT_STAGES = {
    "under contract", "contract pending", "pending", "active under contract",
    "under contract – taking backups", "option period",
}
# Stage labels that indicate a closed/settled deal
CLOSING_STAGES = {
    "closed", "settled", "closed/won", "won", "closing", "sold", "funded",
}


def classify_stage(stage_raw: str) -> str | None:
    """Map a raw FUB stage string to 'contract', 'closing', or None."""
    if not stage_raw:
        return None
    s = stage_raw.lower().strip()
    if s in CLOSING_STAGES:
        return "closing"
    if s in CONTRACT_STAGES:
        return "contract"
    # Fuzzy matches
    if any(k in s for k in ("clos", "settl", "fund", "sold", "won")):
        return "closing"
    if any(k in s for k in ("contract", "pending", "under")):
        return "contract"
    return None


def upsert_deal(fub_deal_id, agent_name, deal_name, sale_price, stage_raw,
                contract_date=None, close_date=None, source="fub_sync",
                commission_pct=None):
    """Insert or update a deal. Returns the classified stage or None if unrecognized."""
    if not is_available():
        return None
    stage = classify_stage(stage_raw)
    if not stage:
        return None  # Skip stages we don't care about

    year = (close_date or contract_date or date.today()).year
    gci_est = None
    if sale_price and commission_pct:
        gci_est = round(float(sale_price) * float(commission_pct), 2)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO deal_log (
                        fub_deal_id, agent_name, deal_name, sale_price,
                        stage, stage_raw, contract_date, close_date,
                        year, gci_estimated, source, synced_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (fub_deal_id) DO UPDATE SET
                        agent_name    = EXCLUDED.agent_name,
                        deal_name     = EXCLUDED.deal_name,
                        sale_price    = EXCLUDED.sale_price,
                        stage         = EXCLUDED.stage,
                        stage_raw     = EXCLUDED.stage_raw,
                        contract_date = COALESCE(EXCLUDED.contract_date, deal_log.contract_date),
                        close_date    = COALESCE(EXCLUDED.close_date,    deal_log.close_date),
                        year          = EXCLUDED.year,
                        gci_estimated = COALESCE(EXCLUDED.gci_estimated, deal_log.gci_estimated),
                        synced_at     = NOW()
                """, (fub_deal_id, agent_name, deal_name, sale_price,
                      stage, stage_raw, contract_date, close_date,
                      year, gci_est, source))
        return stage
    except Exception as e:
        logger.warning("upsert_deal failed: %s", e)
        return None


def log_manual_closing(agent_name, deal_name, sale_price, close_date=None,
                       commission_pct=None):
    """Log a closing entered manually by the manager."""
    if not is_available():
        return False
    if close_date is None:
        close_date = date.today()
    year = close_date.year
    gci_est = round(float(sale_price) * float(commission_pct), 2) if (sale_price and commission_pct) else None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO deal_log
                        (agent_name, deal_name, sale_price, stage, stage_raw,
                         close_date, year, gci_estimated, source)
                    VALUES (%s,%s,%s,'closing','Manual Entry',%s,%s,%s,'manual')
                """, (agent_name, deal_name, sale_price, close_date, year, gci_est))
        return True
    except Exception as e:
        logger.warning("log_manual_closing failed: %s", e)
        return False


def get_deal_summary(agent_name=None, year=None):
    """
    Return contracts and closings count + estimated GCI for a year.
    If agent_name is None, returns summary for ALL agents.
    """
    if not is_available():
        return {}
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if agent_name:
                    cur.execute("""
                        SELECT stage,
                               COUNT(*)              AS deals,
                               SUM(gci_estimated)    AS gci_est,
                               SUM(sale_price)       AS volume
                        FROM   deal_log
                        WHERE  agent_name = %s AND year = %s
                        GROUP  BY stage
                    """, (agent_name, year))
                else:
                    cur.execute("""
                        SELECT agent_name, stage,
                               COUNT(*)           AS deals,
                               SUM(gci_estimated) AS gci_est,
                               SUM(sale_price)    AS volume
                        FROM   deal_log
                        WHERE  year = %s
                        GROUP  BY agent_name, stage
                        ORDER  BY agent_name
                    """, (year,))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []

        if agent_name:
            result = {"contracts": 0, "closings": 0, "gci_est": 0.0, "volume": 0.0}
            for row in rows:
                d = dict(zip(cols, row))
                if d["stage"] == "contract":
                    result["contracts"] = int(d["deals"])
                elif d["stage"] == "closing":
                    result["closings"]  = int(d["deals"])
                    result["gci_est"]   = float(d["gci_est"] or 0)
                    result["volume"]    = float(d["volume"] or 0)
            return result
        else:
            by_agent = {}
            for row in rows:
                d = dict(zip(cols, row))
                a = d["agent_name"]
                if a not in by_agent:
                    by_agent[a] = {"contracts": 0, "closings": 0, "gci_est": 0.0, "volume": 0.0}
                if d["stage"] == "contract":
                    by_agent[a]["contracts"] = int(d["deals"])
                elif d["stage"] == "closing":
                    by_agent[a]["closings"]  = int(d["deals"])
                    by_agent[a]["gci_est"]   = float(d["gci_est"] or 0)
                    by_agent[a]["volume"]    = float(d["volume"] or 0)
            return by_agent
    except Exception as e:
        logger.warning("get_deal_summary failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# YTD actuals cache
# ---------------------------------------------------------------------------

def upsert_ytd_cache(agent_name: str, year: int, calls_ytd: int, appts_ytd: int):
    """Store pre-computed YTD call + appointment counts for an agent."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_ytd_cache (agent_name, year, calls_ytd, appts_ytd, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name, year) DO UPDATE SET
                        calls_ytd  = EXCLUDED.calls_ytd,
                        appts_ytd  = EXCLUDED.appts_ytd,
                        updated_at = NOW()
                """, (agent_name, year, calls_ytd, appts_ytd))
        return True
    except Exception as e:
        logger.warning("upsert_ytd_cache failed: %s", e)
        return False


def get_ytd_cache(year: int = None) -> dict:
    """
    Return cached YTD actuals keyed by agent_name.
    {agent_name: {calls_ytd, appts_ytd, updated_at}}
    """
    if not is_available():
        return {}
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, calls_ytd, appts_ytd, updated_at
                    FROM   agent_ytd_cache
                    WHERE  year = %s
                """, (year,))
                rows = cur.fetchall()
        return {
            r[0]: {"calls_ytd": r[1], "appts_ytd": r[2],
                   "updated_at": r[3].isoformat() if r[3] else None}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_ytd_cache failed: %s", e)
        return {}


def get_cache_updated_at(year: int = None) -> str | None:
    """Return the most recent updated_at timestamp across all agents' cache."""
    if not is_available():
        return None
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(updated_at) FROM agent_ytd_cache WHERE year = %s
                """, (year,))
                row = cur.fetchone()
        return row[0].isoformat() if row and row[0] else None
    except Exception as e:
        logger.warning("get_cache_updated_at failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Scorecard — pace calculation
# ---------------------------------------------------------------------------

def compute_pace(goal: dict, targets: dict, actuals: dict) -> dict:
    """
    Compare actuals against where the agent should be at this point in the year.

    actuals = {
        "calls_ytd":     N,
        "appts_ytd":     N,
        "contracts_ytd": N,
        "closings_ytd":  N,
        "gci_ytd":       N,
    }

    Returns pace dict with pct and status (green/yellow/red) for each metric.
    """
    today = date.today()
    week_num   = today.isocalendar()[1]
    weeks_done = min(week_num, 50)
    pct_of_yr  = weeks_done / 50

    def _pace(actual, annual_target):
        if not annual_target:
            return {"actual": actual, "target_ytd": 0, "annual": 0, "pct": 100, "status": "green"}
        target_ytd = annual_target * pct_of_yr
        pct = round(actual / target_ytd * 100) if target_ytd > 0 else 0
        status = "green" if pct >= 90 else ("yellow" if pct >= 70 else "red")
        return {
            "actual":     actual,
            "target_ytd": round(target_ytd, 1),
            "annual":     annual_target,
            "pct":        pct,
            "status":     status,
        }

    calls_ytd    = actuals.get("calls_ytd", 0)
    appts_ytd    = actuals.get("appts_ytd", 0)
    closings_ytd = actuals.get("closings_ytd", 0)
    gci_ytd      = actuals.get("gci_ytd", 0.0)

    calls_annual    = targets.get("calls_needed_yr", 0)
    appts_annual    = targets.get("appts_needed_yr", 0)
    closings_annual = float(goal.get("gci_goal", 0)) / float(targets.get("avg_commission", 1) or 1)

    calls_pace    = _pace(calls_ytd,    calls_annual)
    appts_pace    = _pace(appts_ytd,    appts_annual)
    closings_pace = _pace(closings_ytd, closings_annual)

    overall_pct = round((calls_pace["pct"] + appts_pace["pct"] + closings_pace["pct"]) / 3)
    overall_status = "green" if overall_pct >= 90 else ("yellow" if overall_pct >= 70 else "red")

    return {
        "week_num":      week_num,
        "pct_of_year":   round(pct_of_yr * 100),
        "calls":         calls_pace,
        "appointments":  appts_pace,
        "closings":      closings_pace,
        "overall_pct":   overall_pct,
        "overall_status": overall_status,
        # Don't flag before week 8 — agents can't be meaningfully behind
        # in the first 7 weeks (Q1 ramp, goal-setting lag, etc.)
        "needs_conversation": week_num >= 8 and overall_pct < 70,
    }
