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
    phone        TEXT,   -- mobile number for Twilio nudges (E.164 format, e.g. +17045551234)
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Add phone column if table already exists (idempotent migration)
DO $$ BEGIN
  ALTER TABLE agent_profiles ADD COLUMN IF NOT EXISTS phone TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
-- fub_phone: phone sourced from FUB roster sync (fallback if agent hasn't self-reported)
DO $$ BEGIN
  ALTER TABLE agent_profiles ADD COLUMN IF NOT EXISTS fub_phone TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
-- onboarding_sent_at: timestamp when goal setup onboarding email was sent (NULL = not yet sent)
DO $$ BEGIN
  ALTER TABLE agent_profiles ADD COLUMN IF NOT EXISTS onboarding_sent_at TIMESTAMPTZ;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

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
    call_to_appt_rate       NUMERIC(5,4)  NOT NULL DEFAULT 0.06,
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

-- Behavioral goal-setting: agent's "why" (Cheplak framework)
CREATE TABLE IF NOT EXISTS agent_why (
    agent_name          TEXT PRIMARY KEY REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    why_statement       TEXT,           -- "If this year goes exactly how I want..."
    who_benefits        TEXT,           -- 'my_kids', 'spouse_partner', 'myself', 'family', 'other'
    who_benefits_custom TEXT,           -- free text when 'other'
    what_happens        TEXT,           -- "What specifically happens for them?"
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Behavioral goal-setting: agent's identity archetype (Atomic Habits)
CREATE TABLE IF NOT EXISTS agent_identity (
    agent_name          TEXT PRIMARY KEY REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    identity_archetype  TEXT,           -- 'consistent', 'closer', 'prospecting_machine', 'relationship_builder', 'comeback_story', 'custom'
    custom_identity     TEXT,           -- free text if custom
    power_hour_time     TIME,           -- "When will you do your power hour?" e.g. 08:30
    daily_calls_target  INTEGER NOT NULL DEFAULT 20,
    daily_texts_target  INTEGER NOT NULL DEFAULT 5,
    daily_appts_target  NUMERIC(4,1) NOT NULL DEFAULT 1.0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Daily activity logging (for streaks and nudge engine)
CREATE TABLE IF NOT EXISTS daily_activity (
    id                  SERIAL PRIMARY KEY,
    agent_name          TEXT    NOT NULL REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    activity_date       DATE    NOT NULL,
    calls_logged        INTEGER NOT NULL DEFAULT 0,
    texts_logged        INTEGER NOT NULL DEFAULT 0,
    appts_logged        NUMERIC(4,1) NOT NULL DEFAULT 0,
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name, activity_date)
);
CREATE INDEX IF NOT EXISTS idx_da_agent_date ON daily_activity (agent_name, activity_date DESC);

-- Streak tracking per agent (updated nightly by scheduler)
CREATE TABLE IF NOT EXISTS streaks (
    agent_name          TEXT PRIMARY KEY REFERENCES agent_profiles(agent_name) ON DELETE CASCADE,
    current_streak      INTEGER NOT NULL DEFAULT 0,
    longest_streak      INTEGER NOT NULL DEFAULT 0,
    last_activity_date  DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Nudge log: track every SMS sent so we don't double-send and can measure opens
CREATE TABLE IF NOT EXISTS nudge_log (
    id              SERIAL PRIMARY KEY,
    agent_name      TEXT    NOT NULL,
    nudge_type      TEXT    NOT NULL,   -- 'morning', 'missed_day', 'streak_break', 'weekly_summary', 'milestone', 'post_closing', 'custom'
    message_content TEXT,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    twilio_sid      TEXT,
    status          TEXT NOT NULL DEFAULT 'sent'   -- 'sent', 'failed', 'preview'
);
CREATE INDEX IF NOT EXISTS idx_nl_agent_type ON nudge_log (agent_name, nudge_type, sent_at DESC);

-- Post-closing follow-up reminders (30/60/90 day)
CREATE TABLE IF NOT EXISTS post_closing_followups (
    id                  SERIAL PRIMARY KEY,
    agent_name          TEXT    NOT NULL,
    fub_deal_id         INTEGER,
    client_name         TEXT,
    close_date          DATE    NOT NULL,
    followup_30_sent    BOOLEAN NOT NULL DEFAULT FALSE,
    followup_60_sent    BOOLEAN NOT NULL DEFAULT FALSE,
    followup_90_sent    BOOLEAN NOT NULL DEFAULT FALSE,
    followup_30_date    DATE,
    followup_60_date    DATE,
    followup_90_date    DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pcf_agent ON post_closing_followups (agent_name, close_date DESC);

-- Scheduler mutex: prevents duplicate job execution across gunicorn workers
CREATE TABLE IF NOT EXISTS scheduler_locks (
    job_name   TEXT PRIMARY KEY,
    locked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
);

-- Archived versions of goals (preserved when agent updates mid-year)
CREATE TABLE IF NOT EXISTS goal_history (
    id                      SERIAL PRIMARY KEY,
    agent_name              TEXT,
    year                    INTEGER,
    gci_goal                NUMERIC(12,2),
    avg_sale_price          NUMERIC(12,2),
    commission_pct          NUMERIC(5,4),
    soi_closings_expected   INTEGER,
    soi_gci_expected        NUMERIC(12,2),
    sphere_touch_monthly    INTEGER,
    call_to_appt_rate       NUMERIC(5,4),
    appt_to_contract_rate   NUMERIC(5,4),
    contract_to_close_rate  NUMERIC(5,4),
    set_by                  TEXT,
    notes                   TEXT,
    archived_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gh_agent_year ON goal_history (agent_name, year, archived_at DESC);
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


def try_acquire_job_lock(job_name: str) -> bool:
    """
    Attempt to acquire a distributed lock for a scheduled job.
    Returns True if the lock was acquired (this worker should run the job).
    Returns False if another worker already holds the lock.
    Stale locks (> 10 min old) are auto-cleared before attempting acquisition.
    """
    if not is_available():
        return True  # No DB — always proceed (single-process mode)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Clear stale locks first
                cur.execute("DELETE FROM scheduler_locks WHERE expires_at < NOW()")
                # Try to insert — ON CONFLICT means another worker holds it
                cur.execute("""
                    INSERT INTO scheduler_locks (job_name, locked_at, expires_at)
                    VALUES (%s, NOW(), NOW() + INTERVAL '10 minutes')
                    ON CONFLICT (job_name) DO NOTHING
                    RETURNING job_name
                """, (job_name,))
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("try_acquire_job_lock failed: %s", e)
        return True  # On error, proceed so jobs don't silently stop


def release_job_lock(job_name: str):
    """Release a previously acquired job lock."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM scheduler_locks WHERE job_name = %s", (job_name,))
    except Exception as e:
        logger.warning("release_job_lock failed: %s", e)


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

def upsert_agent_profile(agent_name, fub_user_id=None, email=None, phone=None, is_active=True):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_profiles (agent_name, fub_user_id, email, phone, is_active, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        fub_user_id = COALESCE(EXCLUDED.fub_user_id, agent_profiles.fub_user_id),
                        email       = COALESCE(EXCLUDED.email,       agent_profiles.email),
                        phone       = COALESCE(EXCLUDED.phone,       agent_profiles.phone),
                        is_active   = EXCLUDED.is_active,
                        updated_at  = NOW()
                """, (agent_name, fub_user_id, email, phone, is_active))
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
                        SELECT agent_name, fub_user_id, email,
                               COALESCE(phone, fub_phone) AS phone,
                               is_active, fub_phone, onboarding_sent_at
                        FROM   agent_profiles WHERE is_active = TRUE ORDER BY agent_name
                    """)
                else:
                    cur.execute("""
                        SELECT agent_name, fub_user_id, email,
                               COALESCE(phone, fub_phone) AS phone,
                               is_active, fub_phone, onboarding_sent_at
                        FROM   agent_profiles ORDER BY agent_name
                    """)
                rows = cur.fetchall()
        return [{"agent_name": r[0], "fub_user_id": r[1], "email": r[2], "phone": r[3],
                 "is_active": r[4], "fub_phone": r[5], "onboarding_sent_at": r[6]}
                for r in rows]
    except Exception as e:
        logger.warning("get_agent_profiles failed: %s", e)
        return []


def upsert_agent_from_fub_roster(agent_name, fub_user_id=None, email=None,
                                  fub_phone=None, is_active=True):
    """
    Upsert agent from FUB roster sync.
    - Only writes to fub_phone (NEVER overwrites user-provided `phone`)
    - Returns True if this is a NEW agent (first time seen)
    Uses INSERT ... ON CONFLICT with a sentinel to detect new inserts atomically.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Use created_at as sentinel: if it equals NOW() it was just inserted
                cur.execute("""
                    INSERT INTO agent_profiles
                        (agent_name, fub_user_id, email, fub_phone, is_active, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        fub_user_id = COALESCE(EXCLUDED.fub_user_id, agent_profiles.fub_user_id),
                        email       = COALESCE(EXCLUDED.email,       agent_profiles.email),
                        fub_phone   = COALESCE(EXCLUDED.fub_phone,   agent_profiles.fub_phone),
                        is_active   = EXCLUDED.is_active,
                        updated_at  = NOW()
                    RETURNING (NOW() - created_at) < INTERVAL '5 seconds' AS is_new
                """, (agent_name, fub_user_id, email, fub_phone, is_active))
                row = cur.fetchone()
                return bool(row and row[0])
    except Exception as e:
        logger.warning("upsert_agent_from_fub_roster failed: %s", e)
        return False


def get_agents_needing_onboarding():
    """Return active agents with an email address who have not yet received the onboarding email."""
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, email, COALESCE(phone, fub_phone) AS phone
                    FROM   agent_profiles
                    WHERE  is_active = TRUE
                      AND  email IS NOT NULL
                      AND  onboarding_sent_at IS NULL
                    ORDER  BY agent_name
                """)
                rows = cur.fetchall()
        return [{"agent_name": r[0], "email": r[1], "phone": r[2]} for r in rows]
    except Exception as e:
        logger.warning("get_agents_needing_onboarding failed: %s", e)
        return []


def mark_onboarding_sent(agent_name):
    """Record that the goal setup onboarding email was sent to this agent."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE agent_profiles SET onboarding_sent_at = NOW()
                    WHERE  agent_name = %s
                """, (agent_name,))
        return True
    except Exception as e:
        logger.warning("mark_onboarding_sent failed: %s", e)
        return False


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
    Auto-renews for 90 days on every valid access so active agents are never locked out.
    Also reactivates tokens expired within the last 60 days — covers accidental
    rotation (e.g. opening the email panel re-created tokens, expiring sent links).
    """
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # First try the normal path (token still valid)
                cur.execute("""
                    UPDATE goal_tokens
                    SET expires_at = GREATEST(expires_at, NOW() + INTERVAL '90 days')
                    WHERE token = %s AND expires_at > NOW()
                    RETURNING agent_name
                """, (token,))
                row = cur.fetchone()
                if row:
                    return row[0]
                # Fallback: reactivate if expired within the last 60 days
                # (handles the case where tokens were rotated while emails were in-flight)
                cur.execute("""
                    UPDATE goal_tokens
                    SET expires_at = NOW() + INTERVAL '90 days'
                    WHERE token = %s
                      AND expires_at > NOW() - INTERVAL '60 days'
                    RETURNING agent_name
                """, (token,))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("resolve_goal_token failed: %s", e)
        return None


# Alias used by nudge_engine.py
def get_goal_token(agent_name):
    return get_token_for_agent(agent_name)


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
                call_to_appt_rate=0.06, appt_to_contract_rate=0.30,
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
    c2a          = float(goal.get("call_to_appt_rate", 0.06))
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


# ---------------------------------------------------------------------------
# Agent Why
# ---------------------------------------------------------------------------

def upsert_agent_why(agent_name, why_statement=None, who_benefits=None,
                     who_benefits_custom=None, what_happens=None):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_why
                        (agent_name, why_statement, who_benefits, who_benefits_custom, what_happens, updated_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        why_statement       = COALESCE(EXCLUDED.why_statement,       agent_why.why_statement),
                        who_benefits        = COALESCE(EXCLUDED.who_benefits,        agent_why.who_benefits),
                        who_benefits_custom = COALESCE(EXCLUDED.who_benefits_custom, agent_why.who_benefits_custom),
                        what_happens        = COALESCE(EXCLUDED.what_happens,        agent_why.what_happens),
                        updated_at          = NOW()
                """, (agent_name, why_statement, who_benefits, who_benefits_custom, what_happens))
        return True
    except Exception as e:
        logger.warning("upsert_agent_why failed: %s", e)
        return False


def get_agent_why(agent_name):
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT why_statement, who_benefits, who_benefits_custom, what_happens
                    FROM agent_why WHERE agent_name = %s
                """, (agent_name,))
                row = cur.fetchone()
        if not row:
            return None
        return {
            "why_statement": row[0],
            "who_benefits": row[1],
            "who_benefits_custom": row[2],
            "what_happens": row[3],
        }
    except Exception as e:
        logger.warning("get_agent_why failed: %s", e)
        return None


def get_all_agent_whys():
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT agent_name, why_statement, who_benefits, who_benefits_custom, what_happens FROM agent_why")
                rows = cur.fetchall()
        return {
            r[0]: {"why_statement": r[1], "who_benefits": r[2],
                   "who_benefits_custom": r[3], "what_happens": r[4]}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_all_agent_whys failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Agent Identity
# ---------------------------------------------------------------------------

def upsert_agent_identity(agent_name, identity_archetype=None, custom_identity=None,
                          power_hour_time=None, daily_calls_target=None,
                          daily_texts_target=None, daily_appts_target=None):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_identity
                        (agent_name, identity_archetype, custom_identity, power_hour_time,
                         daily_calls_target, daily_texts_target, daily_appts_target, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        identity_archetype  = COALESCE(EXCLUDED.identity_archetype,  agent_identity.identity_archetype),
                        custom_identity     = COALESCE(EXCLUDED.custom_identity,     agent_identity.custom_identity),
                        power_hour_time     = COALESCE(EXCLUDED.power_hour_time,     agent_identity.power_hour_time),
                        daily_calls_target  = COALESCE(EXCLUDED.daily_calls_target,  agent_identity.daily_calls_target),
                        daily_texts_target  = COALESCE(EXCLUDED.daily_texts_target,  agent_identity.daily_texts_target),
                        daily_appts_target  = COALESCE(EXCLUDED.daily_appts_target,  agent_identity.daily_appts_target),
                        updated_at          = NOW()
                """, (agent_name, identity_archetype, custom_identity, power_hour_time,
                      daily_calls_target, daily_texts_target, daily_appts_target))
        return True
    except Exception as e:
        logger.warning("upsert_agent_identity failed: %s", e)
        return False


def get_agent_identity(agent_name):
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT identity_archetype, custom_identity, power_hour_time,
                           daily_calls_target, daily_texts_target, daily_appts_target
                    FROM agent_identity WHERE agent_name = %s
                """, (agent_name,))
                row = cur.fetchone()
        if not row:
            return None
        return {
            "identity_archetype": row[0],
            "custom_identity": row[1],
            "power_hour_time": str(row[2]) if row[2] else "08:30",
            "daily_calls_target": row[3] or 20,
            "daily_texts_target": row[4] or 5,
            "daily_appts_target": float(row[5] or 1.0),
        }
    except Exception as e:
        logger.warning("get_agent_identity failed: %s", e)
        return None


def get_all_agent_identities():
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, identity_archetype, custom_identity, power_hour_time,
                           daily_calls_target, daily_texts_target, daily_appts_target
                    FROM agent_identity
                """)
                rows = cur.fetchall()
        return {
            r[0]: {
                "identity_archetype": r[1], "custom_identity": r[2],
                "power_hour_time": str(r[3]) if r[3] else "08:30",
                "daily_calls_target": r[4] or 20,
                "daily_texts_target": r[5] or 5,
                "daily_appts_target": float(r[6] or 1.0),
            }
            for r in rows
        }
    except Exception as e:
        logger.warning("get_all_agent_identities failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Daily Activity Logging
# ---------------------------------------------------------------------------

def log_daily_activity(agent_name, activity_date, calls=0, texts=0, appts=0):
    """Upsert today's activity for an agent."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_activity
                        (agent_name, activity_date, calls_logged, texts_logged, appts_logged, logged_at)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (agent_name, activity_date) DO UPDATE SET
                        calls_logged = EXCLUDED.calls_logged,
                        texts_logged = EXCLUDED.texts_logged,
                        appts_logged = EXCLUDED.appts_logged,
                        logged_at    = NOW()
                """, (agent_name, activity_date, calls, texts, appts))
        return True
    except Exception as e:
        logger.warning("log_daily_activity failed: %s", e)
        return False


def upsert_daily_activity_fub(agent_name, activity_date, calls_fub=0, appts_fub=0):
    """
    Write FUB-sourced activity for a date. Uses GREATEST so a manual entry
    that is higher than the FUB number is never overwritten. Safe to call
    repeatedly — if the agent already logged more calls manually, that wins.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_activity
                        (agent_name, activity_date, calls_logged, appts_logged, logged_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name, activity_date) DO UPDATE SET
                        calls_logged = GREATEST(daily_activity.calls_logged, EXCLUDED.calls_logged),
                        appts_logged = GREATEST(daily_activity.appts_logged, EXCLUDED.appts_logged)
                """, (agent_name, activity_date, calls_fub, appts_fub))
        return True
    except Exception as e:
        logger.warning("upsert_daily_activity_fub failed: %s", e)
        return False


def get_daily_activity(agent_name, days=30):
    """Return daily activity for the last N days, most recent first."""
    if not is_available():
        return []
    try:
        cutoff = date.today() - timedelta(days=days)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT activity_date, calls_logged, texts_logged, appts_logged, logged_at
                    FROM   daily_activity
                    WHERE  agent_name = %s AND activity_date >= %s
                    ORDER  BY activity_date DESC
                """, (agent_name, cutoff))
                rows = cur.fetchall()
        return [
            {
                "date": r[0].isoformat(),
                "calls": r[1], "texts": r[2], "appts": float(r[3] or 0),
                "logged_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("get_daily_activity failed: %s", e)
        return []


def get_todays_activity(agent_name):
    """Return today's logged activity or zeros."""
    if not is_available():
        return {"calls": 0, "texts": 0, "appts": 0}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT calls_logged, texts_logged, appts_logged
                    FROM   daily_activity
                    WHERE  agent_name = %s AND activity_date = %s
                """, (agent_name, date.today()))
                row = cur.fetchone()
        if row:
            return {"calls": row[0], "texts": row[1], "appts": float(row[2] or 0)}
        return {"calls": 0, "texts": 0, "appts": 0}
    except Exception as e:
        logger.warning("get_todays_activity failed: %s", e)
        return {"calls": 0, "texts": 0, "appts": 0}


# ---------------------------------------------------------------------------
# Streaks
# ---------------------------------------------------------------------------

def _recalculate_streak(agent_name, targets: dict) -> dict:
    """
    Recalculate current and longest streaks.
    A day counts if calls_logged >= daily_calls_target.
    Sundays are treated as grace days — a missed Sunday does NOT break the streak.
    """
    if not is_available():
        return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None}
    calls_target = targets.get("daily_calls_target", 1)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT activity_date, calls_logged
                    FROM   daily_activity
                    WHERE  agent_name = %s
                    ORDER  BY activity_date DESC
                    LIMIT  365
                """, (agent_name,))
                rows = cur.fetchall()
        if not rows:
            return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None}

        today = date.today()
        hit_days = {r[0] for r in rows if r[1] >= calls_target}
        last_activity = rows[0][0] if rows else None

        def _is_grace(d):
            """Sundays are grace days — skipped in streak counting."""
            return d.weekday() == 6  # Sunday

        # Current streak — count backwards, skipping Sundays
        current = 0
        check = today
        while True:
            if _is_grace(check):
                check -= timedelta(days=1)
                continue
            if check in hit_days:
                current += 1
                check -= timedelta(days=1)
            else:
                break

        # If nothing counted from today, try from yesterday
        if current == 0:
            check = today - timedelta(days=1)
            while True:
                if _is_grace(check):
                    check -= timedelta(days=1)
                    continue
                if check in hit_days:
                    current += 1
                    check -= timedelta(days=1)
                else:
                    break

        # Longest streak (also skips Sundays)
        longest = 0
        run = 0
        all_dates = sorted(d for d in hit_days)
        for i, d in enumerate(all_dates):
            if i == 0:
                run = 1
            else:
                # Count gap, ignoring Sundays
                prev = all_dates[i - 1]
                gap_days = (d - prev).days
                sundays_between = sum(1 for j in range(1, gap_days)
                                      if _is_grace(prev + timedelta(days=j)))
                effective_gap = gap_days - sundays_between
                if effective_gap == 1:
                    run += 1
                else:
                    run = 1
            longest = max(longest, run)

        return {
            "current_streak": current,
            "longest_streak": max(longest, current),
            "last_activity_date": last_activity.isoformat() if last_activity else None,
        }
    except Exception as e:
        logger.warning("_recalculate_streak failed: %s", e)
        return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None}


def update_streak(agent_name, targets: dict = None):
    """Recalculate and persist streak for one agent."""
    if not is_available():
        return False
    if targets is None:
        ident = get_agent_identity(agent_name)
        targets = {"daily_calls_target": ident["daily_calls_target"]} if ident else {"daily_calls_target": 1}
    streak = _recalculate_streak(agent_name, targets)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO streaks (agent_name, current_streak, longest_streak, last_activity_date, updated_at)
                    VALUES (%s,%s,%s,%s,NOW())
                    ON CONFLICT (agent_name) DO UPDATE SET
                        current_streak     = EXCLUDED.current_streak,
                        longest_streak     = GREATEST(streaks.longest_streak, EXCLUDED.longest_streak),
                        last_activity_date = EXCLUDED.last_activity_date,
                        updated_at         = NOW()
                """, (agent_name, streak["current_streak"], streak["longest_streak"],
                      streak["last_activity_date"]))
        return True
    except Exception as e:
        logger.warning("update_streak failed: %s", e)
        return False


def get_streak(agent_name):
    if not is_available():
        return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT current_streak, longest_streak, last_activity_date, updated_at
                    FROM streaks WHERE agent_name = %s
                """, (agent_name,))
                row = cur.fetchone()
        if not row:
            return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None, "updated_at": None}
        return {
            "current_streak": row[0],
            "longest_streak": row[1],
            "last_activity_date": row[2].isoformat() if row[2] else None,
            "updated_at": row[3].isoformat() if row[3] else None,
        }
    except Exception as e:
        logger.warning("get_streak failed: %s", e)
        return {"current_streak": 0, "longest_streak": 0, "last_activity_date": None}


def get_all_streaks():
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, current_streak, longest_streak, last_activity_date, updated_at
                    FROM streaks
                """)
                rows = cur.fetchall()
        return {
            r[0]: {
                "current_streak": r[1], "longest_streak": r[2],
                "last_activity_date": r[3].isoformat() if r[3] else None,
                "updated_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        }
    except Exception as e:
        logger.warning("get_all_streaks failed: %s", e)
        return {}


def get_team_activity_yesterday():
    """
    Return yesterday's FUB-synced activity for every active agent in one query.
    Used by the morning nudge engine to rank the team without N+1 DB calls.
    Returns dict: { agent_name: {calls, texts, appts, email} }
    """
    if not is_available():
        return {}
    try:
        yesterday = date.today() - timedelta(days=1)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ap.agent_name, ap.email,
                           COALESCE(da.calls_logged, 0) AS calls,
                           COALESCE(da.texts_logged, 0) AS texts,
                           COALESCE(da.appts_logged, 0) AS appts
                    FROM   agent_profiles ap
                    LEFT   JOIN daily_activity da
                           ON  da.agent_name = ap.agent_name
                           AND da.activity_date = %s
                    WHERE  ap.is_active = TRUE AND ap.email IS NOT NULL
                """, (yesterday,))
                rows = cur.fetchall()
        return {
            r[0]: {"email": r[1], "calls": int(r[2] or 0),
                   "texts": int(r[3] or 0), "appts": float(r[4] or 0)}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_team_activity_yesterday failed: %s", e)
        return {}


def get_team_activity_range(start_date, end_date):
    """
    Return summed FUB activity for every active agent over a date range (inclusive).
    Used by weekend nudges for weekly reflection emails.
    Returns dict: { agent_name: {calls, texts, appts, email} }
    """
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ap.agent_name, ap.email,
                           COALESCE(SUM(da.calls_logged), 0) AS calls,
                           COALESCE(SUM(da.texts_logged), 0) AS texts,
                           COALESCE(SUM(da.appts_logged), 0) AS appts
                    FROM   agent_profiles ap
                    LEFT   JOIN daily_activity da
                           ON  da.agent_name = ap.agent_name
                           AND da.activity_date BETWEEN %s AND %s
                    WHERE  ap.is_active = TRUE AND ap.email IS NOT NULL
                    GROUP  BY ap.agent_name, ap.email
                """, (start_date, end_date))
                rows = cur.fetchall()
        return {
            r[0]: {"email": r[1], "calls": int(r[2] or 0),
                   "texts": int(r[3] or 0), "appts": float(r[4] or 0)}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_team_activity_range failed: %s", e)
        return {}


def get_leadstream_top_leads(agent_name, limit=3):
    """
    Return the top N LeadStream leads for an agent from the latest manifest.
    Each lead: {id, name, score, tier, stage}
    """
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT data->'agent'->%s
                    FROM   leadstream_manifest
                    ORDER  BY updated_at DESC LIMIT 1
                """, (agent_name,))
                row = cur.fetchone()
        if not row or not row[0]:
            return []
        leads = row[0] if isinstance(row[0], list) else []
        return leads[:limit]
    except Exception as e:
        logger.warning("get_leadstream_top_leads failed for %s: %s", agent_name, e)
        return []


def get_agents_gone_dark(days=10):
    """
    Return agents who haven't logged ANY activity in `days` days.
    Used for the manager scorecard 'gone dark' alert.
    """
    if not is_available():
        return []
    try:
        cutoff = date.today() - timedelta(days=days)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ap.agent_name, ap.email,
                           COALESCE(ap.phone, ap.fub_phone) AS phone,
                           MAX(da.activity_date) AS last_activity
                    FROM   agent_profiles ap
                    LEFT   JOIN daily_activity da ON da.agent_name = ap.agent_name
                    WHERE  ap.is_active = TRUE
                    GROUP  BY ap.agent_name, ap.email, ap.phone, ap.fub_phone
                    HAVING MAX(da.activity_date) IS NULL
                        OR MAX(da.activity_date) < %s
                    ORDER  BY last_activity ASC NULLS FIRST
                """, (cutoff,))
                rows = cur.fetchall()
        return [
            {"agent_name": r[0], "email": r[1], "phone": r[2],
             "last_activity": r[3].isoformat() if r[3] else None}
            for r in rows
        ]
    except Exception as e:
        logger.warning("get_agents_gone_dark failed: %s", e)
        return []


def get_agents_no_goal_setup():
    """Return active agents who have no goals set (haven't completed goal setup)."""
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ap.agent_name, ap.email,
                           COALESCE(ap.phone, ap.fub_phone) AS phone,
                           ap.onboarding_sent_at
                    FROM   agent_profiles ap
                    LEFT   JOIN goals g ON g.agent_name = ap.agent_name
                                       AND g.year = EXTRACT(YEAR FROM NOW())
                    WHERE  ap.is_active = TRUE
                      AND  (g.agent_name IS NULL OR g.gci_goal = 0)
                    ORDER  BY ap.agent_name
                """)
                rows = cur.fetchall()
        return [
            {"agent_name": r[0], "email": r[1], "phone": r[2],
             "onboarding_sent_at": r[3].isoformat() if r[3] else None}
            for r in rows
        ]
    except Exception as e:
        logger.warning("get_agents_no_goal_setup failed: %s", e)
        return []


def get_agents_no_phone():
    """Return active agents with no phone number in the system (neither user nor FUB)."""
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, email
                    FROM   agent_profiles
                    WHERE  is_active = TRUE
                      AND  phone IS NULL
                      AND  fub_phone IS NULL
                    ORDER  BY agent_name
                """)
                rows = cur.fetchall()
        return [{"agent_name": r[0], "email": r[1]} for r in rows]
    except Exception as e:
        logger.warning("get_agents_no_phone failed: %s", e)
        return []


def save_goal_with_history(agent_name, year, **kwargs):
    """
    Save a goal and archive the previous version in goal_history.
    Ensures we never lose a previous goal when an agent updates mid-year.
    """
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Archive existing goal before overwriting
                cur.execute("""
                    INSERT INTO goal_history (agent_name, year, gci_goal, avg_sale_price,
                        commission_pct, soi_closings_expected, soi_gci_expected,
                        sphere_touch_monthly, call_to_appt_rate, appt_to_contract_rate,
                        contract_to_close_rate, set_by, notes, archived_at)
                    SELECT agent_name, year, gci_goal, avg_sale_price, commission_pct,
                           soi_closings_expected, soi_gci_expected, sphere_touch_monthly,
                           call_to_appt_rate, appt_to_contract_rate, contract_to_close_rate,
                           set_by, notes, NOW()
                    FROM   goals
                    WHERE  agent_name = %s AND year = %s
                """, (agent_name, year))
    except Exception:
        pass  # History table may not exist yet — non-fatal
    return upsert_goal(agent_name, year, **kwargs)


# ---------------------------------------------------------------------------
# Nudge Log
# ---------------------------------------------------------------------------

def log_nudge(agent_name, nudge_type, message_content, twilio_sid=None, status="sent", arc=None):
    """
    Log a sent nudge. If `arc` is provided, prepend 'arc:ARCNAME|' to
    message_content so get_recent_arcs() can reconstruct the arc history.
    """
    if not is_available():
        return False
    try:
        # Encode arc name into message_content so arc history is queryable
        # without a schema change. Format: "arc:ARCNAME|original_content"
        if arc:
            stored_content = "arc:" + arc + "|" + (message_content or "")
        else:
            stored_content = message_content
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO nudge_log (agent_name, nudge_type, message_content, twilio_sid, status)
                    VALUES (%s,%s,%s,%s,%s)
                """, (agent_name, nudge_type, stored_content, twilio_sid, status))
        return True
    except Exception as e:
        logger.warning("log_nudge failed: %s", e)
        return False


def get_last_nudge(agent_name, nudge_type):
    """Return the most recent nudge of a given type for an agent."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sent_at, message_content, status
                    FROM   nudge_log
                    WHERE  agent_name = %s AND nudge_type = %s
                    ORDER  BY sent_at DESC LIMIT 1
                """, (agent_name, nudge_type))
                row = cur.fetchone()
        if not row:
            return None
        return {"sent_at": row[0].isoformat(), "message": row[1], "status": row[2]}
    except Exception as e:
        logger.warning("get_last_nudge failed: %s", e)
        return None


def get_nudge_counts_today(agent_name):
    """How many nudges sent today (ET calendar date) per type — prevents double-sending.

    Uses ET midnight as the day boundary so a manual trigger or late-evening
    send on Monday never blocks Tuesday morning's scheduled run.
    """
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT nudge_type, COUNT(*) FROM nudge_log
                    WHERE  agent_name = %s
                      AND  sent_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'America/New_York')
                                       AT TIME ZONE 'America/New_York'
                    GROUP  BY nudge_type
                """, (agent_name,))
                rows = cur.fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning("get_nudge_counts_today failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Post-closing follow-ups
# ---------------------------------------------------------------------------

def create_post_closing_followup(agent_name, client_name, close_date, fub_deal_id=None):
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO post_closing_followups
                        (agent_name, fub_deal_id, client_name, close_date,
                         followup_30_date, followup_60_date, followup_90_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (agent_name, fub_deal_id, client_name, close_date,
                      close_date + timedelta(days=30),
                      close_date + timedelta(days=60),
                      close_date + timedelta(days=90)))
        return True
    except Exception as e:
        logger.warning("create_post_closing_followup failed: %s", e)
        return False


def get_due_followups(as_of: date = None):
    """Return all unsent follow-ups that are due on or before as_of date."""
    if not is_available():
        return []
    if as_of is None:
        as_of = date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, agent_name, client_name, close_date,
                           followup_30_sent, followup_60_sent, followup_90_sent,
                           followup_30_date, followup_60_date, followup_90_date
                    FROM   post_closing_followups
                    WHERE  (followup_30_sent = FALSE AND followup_30_date <= %s)
                        OR (followup_60_sent = FALSE AND followup_60_date <= %s)
                        OR (followup_90_sent = FALSE AND followup_90_date <= %s)
                    ORDER  BY close_date DESC
                """, (as_of, as_of, as_of))
                rows = cur.fetchall()
        result = []
        for r in rows:
            due = []
            if not r[4] and r[7] and r[7] <= as_of: due.append(30)
            if not r[5] and r[8] and r[8] <= as_of: due.append(60)
            if not r[6] and r[9] and r[9] <= as_of: due.append(90)
            result.append({
                "id": r[0], "agent_name": r[1], "client_name": r[2],
                "close_date": r[3].isoformat(), "due_days": due,
            })
        return result
    except Exception as e:
        logger.warning("get_due_followups failed: %s", e)
        return []


def mark_followup_sent(followup_id, days):
    if not is_available():
        return False
    col = {30: "followup_30_sent", 60: "followup_60_sent", 90: "followup_90_sent"}.get(days)
    if not col:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE post_closing_followups SET {col} = TRUE WHERE id = %s", (followup_id,))
        return True
    except Exception as e:
        logger.warning("mark_followup_sent failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Arc Engine support functions (Phase 4)
# ---------------------------------------------------------------------------

def get_activity_trend(agent_name, days=7):
    """
    Compare the agent's last N days of call activity vs. the prior N days.
    Returns 'improving', 'declining', 'stagnant', or 'unknown'.

    improving  : recent avg > prior avg * 1.15
    declining  : recent avg < prior avg * 0.85
    stagnant   : everything else (including both zero)
    unknown    : not enough data
    """
    if not is_available():
        return "unknown"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT activity_date, calls_logged
                    FROM   daily_activity
                    WHERE  agent_name = %s
                      AND  activity_date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER  BY activity_date DESC
                """ % ("%s", days * 2), (agent_name,))
                rows = cur.fetchall()

        if not rows or len(rows) < days:
            return "unknown"

        # rows are newest-first; split into recent and prior windows
        recent_rows = [r[1] for r in rows[:days]]
        prior_rows  = [r[1] for r in rows[days:days * 2]]

        if not prior_rows:
            return "unknown"

        recent_avg = sum(recent_rows) / len(recent_rows)
        prior_avg  = sum(prior_rows)  / len(prior_rows)

        # Both zero — no data to trend
        if recent_avg == 0 and prior_avg == 0:
            return "unknown"

        # Prior was zero but recent has something — improving
        if prior_avg == 0 and recent_avg > 0:
            return "improving"

        ratio = recent_avg / prior_avg
        if ratio > 1.15:
            return "improving"
        if ratio < 0.85:
            return "declining"
        return "stagnant"

    except Exception as e:
        logger.warning("get_activity_trend failed for %s: %s", agent_name, e)
        return "unknown"


def get_recent_arcs(agent_name, days=7):
    """
    Return arc names used in the last N days for this agent, most recent first.

    Reads from nudge_log where message_content starts with 'arc:ARCNAME|'.
    This encoding is written by log_nudge() when arc= is provided.

    Returns a list of arc name strings, e.g. ['scoreboard', 'identity', 'comeback'].
    """
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT message_content, sent_at
                    FROM   nudge_log
                    WHERE  agent_name = %s
                      AND  nudge_type = 'morning'
                      AND  sent_at   >= NOW() - (%s * INTERVAL '1 day')
                      AND  message_content LIKE 'arc:%%'
                    ORDER  BY sent_at DESC
                """, (agent_name, days))
                rows = cur.fetchall()

        arcs = []
        for row in rows:
            content = row[0] if row else None
            # Format: "arc:ARCNAME|..."
            if content and content.startswith("arc:"):
                rest = content[4:]  # strip "arc:"
                pipe_idx = rest.find("|")
                arc_name = rest[:pipe_idx] if pipe_idx >= 0 else rest
                if arc_name:
                    arcs.append(arc_name)

        return arcs

    except Exception as e:
        logger.warning("get_recent_arcs failed for %s: %s", agent_name, e)
        return []


def get_agent_recent_closings(agent_name, days=60):
    """
    Return recent closings from deal_log for an agent.

    Returns list of dicts: [{deal_name, gci_estimated, close_date}, ...]
    sorted by close_date descending. Used by run_closing_milestones() to
    detect new closings that haven't yet received a milestone email.
    """
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT deal_name, gci_estimated, close_date, id
                    FROM   deal_log
                    WHERE  agent_name = %s
                      AND  stage      = 'closing'
                      AND  close_date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER  BY close_date DESC
                """ % ("%s", days), (agent_name,))
                rows = cur.fetchall()

        result = []
        for (deal_name, gci_estimated, close_date, deal_id) in rows:
            result.append({
                "deal_name":      deal_name or "",
                "gci_estimated":  float(gci_estimated) if gci_estimated else 0.0,
                "close_date":     close_date.isoformat() if close_date else None,
                "deal_id":        deal_id,
            })
        return result

    except Exception as e:
        logger.warning("get_agent_recent_closings failed for %s: %s", agent_name, e)
        return []
