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
-- contact_rate: % of dials that reach a live person (dial → conversation).
-- Added 2026-04 to split the old call_to_appt_rate into two meaningful layers:
--   contact_rate (dials → conversations) × conversation_to_appt_rate (convos → appts)
-- The old call_to_appt_rate column is kept and reinterpreted as conversation_to_appt_rate.
-- Default 0.15 = 15% of dials connect for inbound warm leads (Ylopo/IDX).
DO $$ BEGIN
  ALTER TABLE goals ADD COLUMN IF NOT EXISTS contact_rate NUMERIC(5,4) DEFAULT 0.15;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
  ALTER TABLE goal_history ADD COLUMN IF NOT EXISTS contact_rate NUMERIC(5,4) DEFAULT 0.15;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
-- start_date: when the agent joined the team (used to prorate pace targets mid-year)
DO $$ BEGIN
  ALTER TABLE agent_profiles ADD COLUMN IF NOT EXISTS start_date DATE;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- KPI threshold settings (persisted across Railway deploys via Postgres)
-- Single-row table — always upsert into key='default'.
CREATE TABLE IF NOT EXISTS kpi_settings (
    key         TEXT PRIMARY KEY DEFAULT 'default',
    min_calls   INTEGER NOT NULL DEFAULT 30,
    min_convos  INTEGER NOT NULL DEFAULT 5,
    max_ooc     INTEGER NOT NULL DEFAULT 30,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    convos_ytd   INTEGER NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name, year)
);

-- 1-on-1 meeting brief history (auto-saved each time Barry generates a brief)
CREATE TABLE IF NOT EXISTS meeting_briefs (
    id           SERIAL PRIMARY KEY,
    agent_name   TEXT        NOT NULL,
    week_num     INTEGER,
    year         INTEGER,
    brief_json   JSONB       NOT NULL,
    actuals_json JSONB,
    pace_json    JSONB,
    meta_json    JSONB,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mb_agent ON meeting_briefs (agent_name, generated_at DESC);

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
    convos_logged       INTEGER NOT NULL DEFAULT 0,
    texts_logged        INTEGER NOT NULL DEFAULT 0,
    appts_logged        NUMERIC(4,1) NOT NULL DEFAULT 0,
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name, activity_date)
);
-- Add convos_logged if upgrading from older schema
DO $$ BEGIN
    ALTER TABLE daily_activity ADD COLUMN convos_logged INTEGER NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
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

-- Cross-worker API response cache (persists across Railway deploys + gunicorn workers)
-- Stores full JSON payloads so all workers share the same cached data.
CREATE TABLE IF NOT EXISTS api_cache (
    cache_key   TEXT PRIMARY KEY,
    cached_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    day_key     DATE        NOT NULL DEFAULT CURRENT_DATE,
    data        JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_api_cache_day ON api_cache (day_key DESC);
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

def set_agent_start_date(agent_name: str, start_date) -> bool:
    """Set or update the team start date for an agent. start_date can be a date obj or ISO string."""
    if not is_available():
        return False
    from datetime import date as _date
    if isinstance(start_date, str):
        try:
            start_date = _date.fromisoformat(start_date)
        except ValueError:
            logger.warning("set_agent_start_date: invalid date string %r", start_date)
            return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE agent_profiles SET start_date = %s, updated_at = NOW()
                    WHERE  agent_name = %s
                """, (start_date, agent_name))
        return True
    except Exception as e:
        logger.warning("set_agent_start_date failed for %s: %s", agent_name, e)
        return False


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
                               is_active, fub_phone, onboarding_sent_at, start_date
                        FROM   agent_profiles WHERE is_active = TRUE ORDER BY agent_name
                    """)
                else:
                    cur.execute("""
                        SELECT agent_name, fub_user_id, email,
                               COALESCE(phone, fub_phone) AS phone,
                               is_active, fub_phone, onboarding_sent_at, start_date
                        FROM   agent_profiles ORDER BY agent_name
                    """)
                rows = cur.fetchall()
        return [{"agent_name": r[0], "fub_user_id": r[1], "email": r[2], "phone": r[3],
                 "is_active": r[4], "fub_phone": r[5], "onboarding_sent_at": r[6],
                 "start_date": r[7].isoformat() if r[7] else None}
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
                call_to_appt_rate=0.10, appt_to_contract_rate=0.30,
                contract_to_close_rate=0.80, contact_rate=0.15,
                set_by='manager', notes=None):
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
                        contact_rate, set_by, notes, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
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
                        contact_rate           = EXCLUDED.contact_rate,
                        set_by                 = EXCLUDED.set_by,
                        notes                  = EXCLUDED.notes,
                        updated_at             = NOW()
                    RETURNING *
                """, (agent_name, year, gci_goal, avg_sale_price, commission_pct,
                      soi_closings_expected, soi_gci_expected, sphere_touch_monthly,
                      call_to_appt_rate, appt_to_contract_rate, contract_to_close_rate,
                      contact_rate, set_by, notes))
                row = cur.fetchone()
                cols = [d[0] for d in cur.description]
        return _goal_row_to_dict(row, cols) if row else None
    except Exception as e:
        logger.warning("upsert_goal failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# KPI Settings  (Postgres-backed so they survive Railway deploys)
# ---------------------------------------------------------------------------

def load_kpi_settings():
    """Load KPI thresholds from Postgres. Returns dict or {} if unavailable."""
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT min_calls, min_convos, max_ooc FROM kpi_settings WHERE key='default'")
                row = cur.fetchone()
        if row:
            return {"min_calls": row[0], "min_convos": row[1], "max_ooc": row[2]}
    except Exception as e:
        logger.warning("load_kpi_settings failed: %s", e)
    return {}


def save_kpi_settings(min_calls, min_convos, max_ooc):
    """Persist KPI thresholds to Postgres. Returns True on success."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kpi_settings (key, min_calls, min_convos, max_ooc, updated_at)
                    VALUES ('default', %s, %s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET
                        min_calls  = EXCLUDED.min_calls,
                        min_convos = EXCLUDED.min_convos,
                        max_ooc    = EXCLUDED.max_ooc,
                        updated_at = NOW()
                """, (min_calls, min_convos, max_ooc))
        return True
    except Exception as e:
        logger.warning("save_kpi_settings failed: %s", e)
        return False


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

    Two-layer call model:
      Dials → Conversations  (contact_rate:           % of dials that reach a live person)
      Conversations → Appts  (call_to_appt_rate:      % of conversations that book an appt)

    call_to_appt_rate was historically mislabeled as a dial-to-appointment rate (6% default).
    It is now correctly treated as conversation-to-appointment rate (10% default).
    contact_rate = 15% is a realistic default for inbound warm leads (Ylopo/IDX).

    Returns a dict of derived targets (not stored in DB).
    """
    gci          = float(goal.get("gci_goal", 0))
    avg_price    = float(goal.get("avg_sale_price", 400000))
    comm_pct     = float(goal.get("commission_pct", 0.025))
    soi_close    = int(goal.get("soi_closings_expected", 0))
    soi_gci      = float(goal.get("soi_gci_expected", 0))
    # conversation → appointment rate (was call_to_appt_rate, default updated 0.06 → 0.10)
    c2a          = float(goal.get("call_to_appt_rate", 0.10))
    a2c          = float(goal.get("appt_to_contract_rate", 0.30))
    c2cl         = float(goal.get("contract_to_close_rate", 0.80))
    # contact_rate: % of dials that become live conversations (new field, default 0.15)
    contact_r    = float(goal.get("contact_rate", 0.15))

    avg_commission   = avg_price * comm_pct
    closings_needed  = round(gci / avg_commission, 1) if avg_commission > 0 else 0
    prospect_close   = max(closings_needed - soi_close, 0)
    contracts_needed = round(prospect_close / c2cl, 1) if c2cl > 0 else 0
    appts_needed_yr  = round(contracts_needed / a2c, 1) if a2c > 0 else 0
    # Two-layer: appts → convos → dials
    convos_needed_yr = round(appts_needed_yr / c2a, 1) if c2a > 0 else 0
    dials_needed_yr  = round(convos_needed_yr / contact_r, 1) if contact_r > 0 else 0

    appts_per_week   = round(appts_needed_yr / 50, 1)
    convos_per_week  = round(convos_needed_yr / 50, 1)
    dials_per_week   = round(dials_needed_yr / 50, 1)

    return {
        "avg_commission":    round(avg_commission, 0),
        "closings_needed":   closings_needed,
        "soi_closings":      soi_close,
        "prospect_closings": prospect_close,
        "contracts_needed":  contracts_needed,
        "appts_needed_yr":   appts_needed_yr,
        "convos_needed_yr":  convos_needed_yr,
        "dials_needed_yr":   dials_needed_yr,
        "appts_per_week":    appts_per_week,
        "convos_per_week":   convos_per_week,
        "dials_per_week":    dials_per_week,
        # Backward-compat aliases so existing callers don't break
        "calls_needed_yr":   dials_needed_yr,
        "calls_per_week":    dials_per_week,
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


def classify_stage(stage_raw: str) -> "str | None":
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

def upsert_ytd_cache(agent_name: str, year: int, calls_ytd: int, appts_ytd: int,
                     convos_ytd: int = 0):
    """Store pre-computed YTD call + appointment + conversation counts for an agent."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Safe migration: add convos_ytd column if it doesn't exist yet
                cur.execute("""
                    DO $$ BEGIN
                        ALTER TABLE agent_ytd_cache ADD COLUMN convos_ytd INTEGER NOT NULL DEFAULT 0;
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)
                cur.execute("""
                    INSERT INTO agent_ytd_cache (agent_name, year, calls_ytd, appts_ytd, convos_ytd, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name, year) DO UPDATE SET
                        calls_ytd  = EXCLUDED.calls_ytd,
                        appts_ytd  = EXCLUDED.appts_ytd,
                        convos_ytd = EXCLUDED.convos_ytd,
                        updated_at = NOW()
                """, (agent_name, year, calls_ytd, appts_ytd, convos_ytd))
        return True
    except Exception as e:
        logger.warning("upsert_ytd_cache failed: %s", e)
        return False


def get_ytd_cache(year: int = None) -> dict:
    """
    Return cached YTD actuals keyed by agent_name.
    {agent_name: {calls_ytd, appts_ytd, convos_ytd, updated_at}}
    """
    if not is_available():
        return {}
    if year is None:
        year = datetime.now().year
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, calls_ytd, appts_ytd,
                           COALESCE(convos_ytd, 0) AS convos_ytd, updated_at
                    FROM   agent_ytd_cache
                    WHERE  year = %s
                """, (year,))
                rows = cur.fetchall()
        return {
            r[0]: {"calls_ytd": r[1], "appts_ytd": r[2], "convos_ytd": r[3],
                   "updated_at": r[4].isoformat() if r[4] else None}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_ytd_cache failed: %s", e)
        return {}


def get_cache_updated_at(year: int = None) -> "str | None":
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
# 1-on-1 meeting brief history
# ---------------------------------------------------------------------------

def save_meeting_brief(agent_name: str, week_num: int, year: int,
                       brief: dict, actuals: dict = None,
                       pace: dict = None, meta: dict = None) -> "int | None":
    """Save a generated meeting brief. Returns the new row id, or None on failure."""
    if not is_available():
        return None
    import json as _json
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO meeting_briefs
                        (agent_name, week_num, year, brief_json, actuals_json, pace_json, meta_json, generated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id
                """, (
                    agent_name, week_num, year,
                    _json.dumps(brief),
                    _json.dumps(actuals or {}),
                    _json.dumps(pace   or {}),
                    _json.dumps(meta   or {}),
                ))
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("save_meeting_brief failed for %s: %s", agent_name, e)
        return None


def delete_meeting_brief(brief_id: int) -> bool:
    """Delete a saved meeting brief by id."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM meeting_briefs WHERE id = %s", (brief_id,))
        return True
    except Exception as e:
        logger.warning("delete_meeting_brief failed for id %s: %s", brief_id, e)
        return False


def get_meeting_briefs(agent_name: str, limit: int = 20) -> list:
    """Return past meeting briefs for an agent, newest first."""
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, agent_name, week_num, year, brief_json,
                           actuals_json, pace_json, meta_json, generated_at
                    FROM   meeting_briefs
                    WHERE  agent_name = %s
                    ORDER  BY generated_at DESC
                    LIMIT  %s
                """, (agent_name, limit))
                rows = cur.fetchall()
        result = []
        import json as _json
        for r in rows:
            result.append({
                "id":           r[0],
                "agent_name":   r[1],
                "week_num":     r[2],
                "year":         r[3],
                "brief":        r[4] if isinstance(r[4], dict) else _json.loads(r[4] or "{}"),
                "actuals":      r[5] if isinstance(r[5], dict) else _json.loads(r[5] or "{}"),
                "pace":         r[6] if isinstance(r[6], dict) else _json.loads(r[6] or "{}"),
                "meta":         r[7] if isinstance(r[7], dict) else _json.loads(r[7] or "{}"),
                "generated_at": r[8].isoformat() if r[8] else None,
            })
        return result
    except Exception as e:
        logger.warning("get_meeting_briefs failed for %s: %s", agent_name, e)
        return []


# ---------------------------------------------------------------------------
# Scorecard — pace calculation
# ---------------------------------------------------------------------------

def compute_pace(goal: dict, targets: dict, actuals: dict, start_date=None) -> dict:
    """
    Compare actuals against where the agent should be at this point in the year.

    actuals = {
        "calls_ytd":     N,
        "convos_ytd":    N,
        "appts_ytd":     N,
        "contracts_ytd": N,
        "closings_ytd":  N,
        "gci_ytd":       N,
    }

    start_date: date the agent joined the team (date obj or ISO string).
    When set and within the current year, all annual targets are prorated to the
    agent's tenure window and pace is measured from their start date — not Jan 1.
    This prevents a 30-day agent from looking like they're 90% behind their goals.

    Returns pace dict with pct and status (green/yellow/red) for each metric.
    """
    today    = date.today()
    year     = today.year
    year_start = date(year, 1, 1)
    year_end   = date(year, 12, 31)
    WORK_WEEKS = 50  # standard working-year length

    # ── Parse start_date ────────────────────────────────────────────────────
    if isinstance(start_date, str):
        try:
            start_date = date.fromisoformat(start_date)
        except (ValueError, TypeError):
            start_date = None

    # Only apply tenure proration when the agent actually started mid-year
    if start_date and start_date.year == year and start_date > year_start:
        # How many working weeks do they have available this year?
        tenure_weeks_total = max((year_end - start_date).days / 7, 1)
        tenure_weeks_done  = max((today - start_date).days / 7, 0)
        pct_of_tenure  = min(tenure_weeks_done / tenure_weeks_total, 1.0)
        # Scale annual targets: an agent starting week 20 of 50 has 30/50 = 60% of the year
        tenure_scale   = tenure_weeks_total / WORK_WEEKS
        weeks_on_team  = round(tenure_weeks_done)
        is_new_agent   = True
    else:
        week_num       = today.isocalendar()[1]
        weeks_done     = min(week_num, WORK_WEEKS)
        pct_of_tenure  = weeks_done / WORK_WEEKS
        tenure_scale   = 1.0
        weeks_on_team  = None
        is_new_agent   = False

    def _pace(actual, annual_target):
        if not annual_target:
            return {"actual": actual, "target_ytd": 0, "annual": 0, "pct": 100, "status": "green"}
        # Prorate annual target to agent's available tenure window
        adjusted_annual = annual_target * tenure_scale
        target_ytd = adjusted_annual * pct_of_tenure
        pct = round(actual / target_ytd * 100) if target_ytd > 0 else 0
        status = "green" if pct >= 90 else ("yellow" if pct >= 70 else "red")
        return {
            "actual":           actual,
            "target_ytd":       round(target_ytd, 1),
            "annual":           round(adjusted_annual, 1),   # prorated annual
            "annual_full":      annual_target,               # original full-year target
            "pct":              pct,
            "status":           status,
        }

    calls_ytd    = actuals.get("calls_ytd", 0)
    convos_ytd   = actuals.get("convos_ytd", 0)
    appts_ytd    = actuals.get("appts_ytd", 0)
    closings_ytd = actuals.get("closings_ytd", 0)
    gci_ytd      = actuals.get("gci_ytd", 0.0)

    # Pace the funnel: conversations → appointments → closings
    convos_annual   = targets.get("convos_needed_yr", 0)
    appts_annual    = targets.get("appts_needed_yr", 0)
    closings_annual = float(goal.get("gci_goal", 0)) / float(targets.get("avg_commission", 1) or 1)

    # Fallback: if no convos tracked yet, estimate from dials at contact rate
    if convos_ytd == 0 and calls_ytd > 0:
        contact_r = float(goal.get("contact_rate", 0.15))
        convos_ytd_eff = round(calls_ytd * contact_r)
    else:
        convos_ytd_eff = convos_ytd

    convos_pace   = _pace(convos_ytd_eff, convos_annual)
    appts_pace    = _pace(appts_ytd,      appts_annual)
    closings_pace = _pace(closings_ytd,   closings_annual)

    overall_pct    = round((convos_pace["pct"] + appts_pace["pct"] + closings_pace["pct"]) / 3)
    overall_status = "green" if overall_pct >= 90 else ("yellow" if overall_pct >= 70 else "red")

    # Determine "weeks on team" for display / coaching context
    display_week_num = today.isocalendar()[1] if not is_new_agent else None

    # Don't flag new agents as needing a coaching conversation until week 4 on team
    weeks_elapsed_on_team = weeks_on_team if is_new_agent else min(today.isocalendar()[1], 50)
    needs_convo = weeks_elapsed_on_team is not None and weeks_elapsed_on_team >= 4 and overall_pct < 70

    return {
        "week_num":       display_week_num,
        "weeks_on_team":  weeks_on_team,       # None if full-year agent
        "is_new_agent":   is_new_agent,
        "pct_of_tenure":  round(pct_of_tenure * 100),
        "tenure_scale":   round(tenure_scale, 3),
        "convos":         convos_pace,          # primary activity metric
        "calls_ytd":      calls_ytd,            # raw dials — context only
        "appointments":   appts_pace,
        "closings":       closings_pace,
        "overall_pct":    overall_pct,
        "overall_status": overall_status,
        "needs_conversation": needs_convo,
        "calls":          convos_pace,          # backward-compat alias
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


def upsert_daily_activity_fub(agent_name, activity_date, calls_fub=0, appts_fub=0, convos_fub=0):
    """
    Write FUB-sourced activity for a date.
    - calls_fub  : outbound calls only (isIncoming==False)
    - convos_fub : calls with duration >= CONVERSATION_THRESHOLD_SECONDS
    - appts_fub  : appointments set
    Uses GREATEST so a manual entry that is higher than FUB is never overwritten.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_activity
                        (agent_name, activity_date, calls_logged, convos_logged, appts_logged, logged_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (agent_name, activity_date) DO UPDATE SET
                        calls_logged  = GREATEST(daily_activity.calls_logged,  EXCLUDED.calls_logged),
                        convos_logged = GREATEST(daily_activity.convos_logged, EXCLUDED.convos_logged),
                        appts_logged  = GREATEST(daily_activity.appts_logged,  EXCLUDED.appts_logged)
                """, (agent_name, activity_date, calls_fub, convos_fub, appts_fub))
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
        return {"calls": 0, "convos": 0, "texts": 0, "appts": 0}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT calls_logged, COALESCE(convos_logged, 0), texts_logged, appts_logged
                    FROM   daily_activity
                    WHERE  agent_name = %s AND activity_date = %s
                """, (agent_name, date.today()))
                row = cur.fetchone()
        if row:
            return {"calls": row[0], "convos": row[1], "texts": row[2], "appts": float(row[3] or 0)}
        return {"calls": 0, "convos": 0, "texts": 0, "appts": 0}
    except Exception as e:
        logger.warning("get_todays_activity failed: %s", e)
        return {"calls": 0, "convos": 0, "texts": 0, "appts": 0}


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
                           COALESCE(da.calls_logged, 0)  AS calls,
                           COALESCE(da.convos_logged, 0) AS convos,
                           COALESCE(da.texts_logged, 0)  AS texts,
                           COALESCE(da.appts_logged, 0)  AS appts
                    FROM   agent_profiles ap
                    LEFT   JOIN daily_activity da
                           ON  da.agent_name = ap.agent_name
                           AND da.activity_date = %s
                    WHERE  ap.is_active = TRUE AND ap.email IS NOT NULL
                """, (yesterday,))
                rows = cur.fetchall()
        return {
            r[0]: {"email": r[1], "calls": int(r[2] or 0), "convos": int(r[3] or 0),
                   "texts": int(r[4] or 0), "appts": float(r[5] or 0)}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_team_activity_yesterday failed: %s", e)
        return {}


def get_all_ytd_from_daily_activity(year: int = None) -> dict:
    """
    Compute YTD calls, convos, and appts for ALL agents in one query.
    Used to fill in team rank when ytd_cache only has a subset of agents.
    Returns: { agent_name: {calls_ytd, convos_ytd, appts_ytd} }
    """
    if not is_available():
        return {}
    if year is None:
        year = datetime.now().year
    jan1 = date(year, 1, 1)
    today = date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name,
                           COALESCE(SUM(calls_logged),  0),
                           COALESCE(SUM(convos_logged), 0),
                           COALESCE(SUM(appts_logged),  0)
                    FROM   daily_activity
                    WHERE  activity_date BETWEEN %s AND %s
                    GROUP  BY agent_name
                """, (jan1, today))
                rows = cur.fetchall()
        return {
            r[0]: {"calls_ytd": int(r[1] or 0),
                   "convos_ytd": int(r[2] or 0),
                   "appts_ytd":  int(r[3] or 0)}
            for r in rows
        }
    except Exception as e:
        logger.warning("get_all_ytd_from_daily_activity failed: %s", e)
        return {}


def get_ytd_from_daily_activity(agent_name: str, year: int = None) -> dict:
    """
    Compute YTD calls, convos, and appts for one agent by summing daily_activity rows.
    Used as a fallback when agent_ytd_cache is empty (e.g. before the first goals sync runs).
    """
    if not is_available():
        return {"calls_ytd": 0, "convos_ytd": 0, "appts_ytd": 0}
    if year is None:
        year = datetime.now().year
    jan1 = date(year, 1, 1)
    today = date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(calls_logged),  0),
                           COALESCE(SUM(convos_logged), 0),
                           COALESCE(SUM(appts_logged),  0)
                    FROM   daily_activity
                    WHERE  agent_name   = %s
                      AND  activity_date BETWEEN %s AND %s
                """, (agent_name, jan1, today))
                row = cur.fetchone()
        if row:
            return {
                "calls_ytd":  int(row[0] or 0),
                "convos_ytd": int(row[1] or 0),
                "appts_ytd":  int(row[2] or 0),
            }
        return {"calls_ytd": 0, "convos_ytd": 0, "appts_ytd": 0}
    except Exception as e:
        logger.warning("get_ytd_from_daily_activity failed for %s: %s", agent_name, e)
        return {"calls_ytd": 0, "convos_ytd": 0, "appts_ytd": 0}


def get_agent_activity_context(agent_name: str) -> dict:
    """
    Single DB query covering the past 70 days, aggregated in Python into:
      - windows: {this_week, last_week, mtd, last_month}  — each {calls, convos, appts}
      - weekly_trend: list of 9 weekly buckets oldest→newest
          each: {week_start (ISO), week_label, calls, convos, appts, is_current}
    Used by the 1-on-1 meeting brief for recent momentum context.
    """
    if not is_available():
        return {"windows": {}, "weekly_trend": []}

    today = date.today()
    since = today - timedelta(days=70)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT activity_date,
                           COALESCE(calls_logged, 0),
                           COALESCE(convos_logged, 0),
                           COALESCE(appts_logged, 0)
                    FROM   daily_activity
                    WHERE  agent_name   = %s
                      AND  activity_date BETWEEN %s AND %s
                    ORDER  BY activity_date
                """, (agent_name, since, today))
                rows = cur.fetchall()
    except Exception as e:
        logger.warning("get_agent_activity_context failed for %s: %s", agent_name, e)
        return {"windows": {}, "weekly_trend": []}

    # ── Index rows by date ──────────────────────────────────────────────────
    by_date = {}
    for activity_date, calls, convos, appts in rows:
        by_date[activity_date] = (int(calls), int(convos), float(appts))

    def _sum_range(start, end):
        """Sum calls/convos/appts for date range [start, end] inclusive."""
        calls = convos = appts = 0
        d = start
        while d <= end:
            if d in by_date:
                c, cv, a = by_date[d]
                calls += c; convos += cv; appts += a
            d += timedelta(days=1)
        return {"calls": calls, "convos": convos, "appts": round(appts, 1)}

    # ── Calendar boundaries ─────────────────────────────────────────────────
    days_since_mon   = today.weekday()          # 0=Mon, 6=Sun
    this_week_start  = today - timedelta(days=days_since_mon)
    last_week_end    = this_week_start - timedelta(days=1)
    last_week_start  = last_week_end   - timedelta(days=6)

    mtd_start        = date(today.year, today.month, 1)
    if today.month == 1:
        lm_start = date(today.year - 1, 12, 1)
        lm_end   = date(today.year, 1, 1) - timedelta(days=1)
    else:
        lm_start = date(today.year, today.month - 1, 1)
        lm_end   = mtd_start - timedelta(days=1)

    windows = {
        "this_week":  _sum_range(this_week_start,  today),
        "last_week":  _sum_range(last_week_start,  last_week_end),
        "mtd":        _sum_range(mtd_start,         today),
        "last_month": _sum_range(lm_start,          lm_end),
    }

    # ── 9 weekly buckets (Mon-Sun), oldest→newest ───────────────────────────
    weekly_trend = []
    for i in range(8, -1, -1):
        wk_start = this_week_start - timedelta(weeks=i)
        wk_end   = wk_start + timedelta(days=6)
        wk_end   = min(wk_end, today)
        label    = wk_start.strftime("%-m/%-d") + "–" + wk_end.strftime("%-m/%-d")
        s        = _sum_range(wk_start, wk_end)
        weekly_trend.append({
            "week_start":  wk_start.isoformat(),
            "week_label":  label,
            "calls":       s["calls"],
            "convos":      s["convos"],
            "appts":       s["appts"],
            "is_current":  (i == 0),
        })

    # ── Trend direction: compare last 3 complete weeks vs prior 4 ──────────
    complete_weeks = [w for w in weekly_trend if not w["is_current"]]
    recent3  = complete_weeks[-3:] if len(complete_weeks) >= 3 else complete_weeks
    prior4   = complete_weeks[-7:-3] if len(complete_weeks) >= 7 else complete_weeks[:max(0, len(complete_weeks)-3)]
    avg_r = (sum(w["convos"] for w in recent3) / len(recent3)) if recent3 else 0
    avg_p = (sum(w["convos"] for w in prior4)  / len(prior4))  if prior4  else 0
    if avg_p == 0:
        trend_dir = "insufficient_data"
    elif avg_r >= avg_p * 1.15:
        trend_dir = "building"
    elif avg_r <= avg_p * 0.80:
        trend_dir = "backing_off"
    else:
        trend_dir = "steady"

    return {
        "windows":      windows,
        "weekly_trend": weekly_trend,
        "trend_dir":    trend_dir,
        "avg_recent":   round(avg_r, 1),
        "avg_prior":    round(avg_p, 1),
    }


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
                           COALESCE(SUM(da.calls_logged),  0) AS calls,
                           COALESCE(SUM(da.convos_logged), 0) AS convos,
                           COALESCE(SUM(da.texts_logged),  0) AS texts,
                           COALESCE(SUM(da.appts_logged),  0) AS appts
                    FROM   agent_profiles ap
                    LEFT   JOIN daily_activity da
                           ON  da.agent_name = ap.agent_name
                           AND da.activity_date BETWEEN %s AND %s
                    WHERE  ap.is_active = TRUE AND ap.email IS NOT NULL
                    GROUP  BY ap.agent_name, ap.email
                """, (start_date, end_date))
                rows = cur.fetchall()
        return {
            r[0]: {"email": r[1], "calls": int(r[2] or 0), "convos": int(r[3] or 0),
                   "texts": int(r[4] or 0), "appts": float(r[5] or 0)}
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
                        contract_to_close_rate, contact_rate, set_by, notes, archived_at)
                    SELECT agent_name, year, gci_goal, avg_sale_price, commission_pct,
                           soi_closings_expected, soi_gci_expected, sphere_touch_monthly,
                           call_to_appt_rate, appt_to_contract_rate, contract_to_close_rate,
                           COALESCE(contact_rate, 0.15), set_by, notes, NOW()
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


# ---------------------------------------------------------------------------
# Weekly KPI Snapshots  (completed-week cache for week-over-week trends)
# ---------------------------------------------------------------------------

def ensure_weekly_kpi_snapshots_table():
    """Create weekly_kpi_snapshots table if it doesn't exist."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weekly_kpi_snapshots (
                        id               SERIAL PRIMARY KEY,
                        week_start       DATE NOT NULL,
                        week_end         DATE NOT NULL,
                        agent_name       TEXT NOT NULL,
                        agent_user_id    INTEGER,
                        outbound_calls   INTEGER DEFAULT 0,
                        conversations    INTEGER DEFAULT 0,
                        talk_time_secs   INTEGER DEFAULT 0,
                        appts_set        INTEGER DEFAULT 0,
                        appts_met        INTEGER DEFAULT 0,
                        texts_out        INTEGER DEFAULT 0,
                        texts_in         INTEGER DEFAULT 0,
                        call_to_convo    INTEGER DEFAULT 0,
                        convo_to_appt    INTEGER DEFAULT 0,
                        kpi_pass         BOOLEAN DEFAULT FALSE,
                        captured_at      TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(week_start, agent_name)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_wks_week_start
                    ON weekly_kpi_snapshots(week_start DESC)
                """)
    except Exception as e:
        logger.warning("ensure_weekly_kpi_snapshots_table failed: %s", e)


def save_weekly_kpi_snapshot(week_start, week_end, agents):
    """
    Upsert per-agent KPI metrics for a completed work week.

    week_start / week_end: datetime.date objects (Monday / Saturday).
    agents: list of agent dicts from run_audit_data() — each has 'name',
            'user_id', 'metrics', 'evaluation', 'call_to_convo', 'convo_to_appt'.
    """
    if not is_available():
        return
    try:
        ensure_weekly_kpi_snapshots_table()
        with get_conn() as conn:
            with conn.cursor() as cur:
                for a in agents:
                    m  = a.get("metrics", {})
                    ev = a.get("evaluation", {})
                    cur.execute("""
                        INSERT INTO weekly_kpi_snapshots
                            (week_start, week_end, agent_name, agent_user_id,
                             outbound_calls, conversations, talk_time_secs,
                             appts_set, appts_met, texts_out, texts_in,
                             call_to_convo, convo_to_appt, kpi_pass, captured_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (week_start, agent_name) DO UPDATE SET
                            week_end       = EXCLUDED.week_end,
                            agent_user_id  = EXCLUDED.agent_user_id,
                            outbound_calls = EXCLUDED.outbound_calls,
                            conversations  = EXCLUDED.conversations,
                            talk_time_secs = EXCLUDED.talk_time_secs,
                            appts_set      = EXCLUDED.appts_set,
                            appts_met      = EXCLUDED.appts_met,
                            texts_out      = EXCLUDED.texts_out,
                            texts_in       = EXCLUDED.texts_in,
                            call_to_convo  = EXCLUDED.call_to_convo,
                            convo_to_appt  = EXCLUDED.convo_to_appt,
                            kpi_pass       = EXCLUDED.kpi_pass,
                            captured_at    = NOW()
                    """, (
                        week_start, week_end,
                        a["name"], a.get("user_id"),
                        m.get("outbound_calls", 0),
                        m.get("conversations", 0),
                        m.get("talk_time_seconds", 0),
                        m.get("appts_set", 0),
                        m.get("appts_met", 0),
                        m.get("texts_out", 0),
                        m.get("texts_in", 0),
                        a.get("call_to_convo", 0),
                        a.get("convo_to_appt", 0),
                        ev.get("overall_pass", False),
                    ))
    except Exception as e:
        logger.warning("save_weekly_kpi_snapshot failed: %s", e)


def get_weekly_kpi_history(weeks=8):
    """
    Return per-agent KPI snapshots for the last `weeks` completed work weeks,
    newest first. Result format:

    [
      {
        "week_start": "2026-04-07",
        "week_end":   "2026-04-12",
        "agents": [
          {"name": "Joe", "outbound_calls": 42, "conversations": 12, ...},
          ...
        ]
      },
      ...
    ]
    """
    if not is_available():
        return []
    try:
        ensure_weekly_kpi_snapshots_table()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT week_start, week_end, agent_name, agent_user_id,
                           outbound_calls, conversations, talk_time_secs,
                           appts_set, appts_met, texts_out, texts_in,
                           call_to_convo, convo_to_appt, kpi_pass, captured_at
                    FROM weekly_kpi_snapshots
                    WHERE week_start >= (
                        SELECT COALESCE(MIN(week_start), CURRENT_DATE - 365)
                        FROM (
                            SELECT DISTINCT week_start
                            FROM weekly_kpi_snapshots
                            ORDER BY week_start DESC
                            LIMIT %s
                        ) sub
                    )
                    ORDER BY week_start DESC, agent_name ASC
                """, (weeks,))
                rows = cur.fetchall()

        # Group by week
        from collections import OrderedDict
        weeks_map = OrderedDict()
        for row in rows:
            ws = row[0].isoformat()
            if ws not in weeks_map:
                weeks_map[ws] = {
                    "week_start": ws,
                    "week_end": row[1].isoformat(),
                    "agents": []
                }
            weeks_map[ws]["agents"].append({
                "name":           row[2],
                "user_id":        row[3],
                "outbound_calls": row[4],
                "conversations":  row[5],
                "talk_time_secs": row[6],
                "appts_set":      row[7],
                "appts_met":      row[8],
                "texts_out":      row[9],
                "texts_in":       row[10],
                "call_to_convo":  row[11],
                "convo_to_appt":  row[12],
                "kpi_pass":       row[13],
                "captured_at":    row[14].isoformat() if row[14] else None,
            })
        return list(weeks_map.values())
    except Exception as e:
        logger.warning("get_weekly_kpi_history failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Calls Cache  (incremental FUB call sync — avoids 2000-record offset cap)
# ---------------------------------------------------------------------------

def ensure_calls_cache_table():
    """Create calls_cache table if not exists."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS calls_cache (
                        fub_call_id  INTEGER     PRIMARY KEY,
                        user_id      INTEGER,
                        person_id    INTEGER,
                        created      TIMESTAMPTZ NOT NULL,
                        duration     INTEGER,
                        is_outbound  BOOLEAN,
                        direction    TEXT,
                        synced_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_calls_cache_created
                        ON calls_cache (created DESC);
                    CREATE INDEX IF NOT EXISTS idx_calls_cache_user_id
                        ON calls_cache (user_id, created DESC);
                """)
            conn.commit()
    except Exception as e:
        logger.warning("ensure_calls_cache_table failed: %s", e)


def upsert_calls_cache(calls):
    """Bulk upsert a list of FUB call dicts into calls_cache.

    Accepts FUB field names: id, userId, personId, created, duration,
    isOutbound, direction.  Returns count of rows upserted.
    """
    if not is_available() or not calls:
        return 0
    try:
        import psycopg2.extras
        rows = []
        for c in calls:
            rows.append((
                c.get("id"),
                c.get("userId"),
                c.get("personId"),
                c.get("created"),
                c.get("duration"),
                c.get("isOutbound"),
                c.get("direction"),
            ))
        with get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO calls_cache
                        (fub_call_id, user_id, person_id, created,
                         duration, is_outbound, direction, synced_at)
                    VALUES %s
                    ON CONFLICT (fub_call_id) DO UPDATE
                        SET synced_at = NOW()
                    """,
                    rows,
                    template="(%s, %s, %s, %s::timestamptz, %s, %s, %s, NOW())",
                )
                count = cur.rowcount
            conn.commit()
        return count
    except Exception as e:
        logger.warning("upsert_calls_cache failed: %s", e)
        return 0


def get_cached_calls(since, until=None):
    """Return cached calls between since and until as list of dicts.

    Returns dicts with original FUB field names (id, userId, personId,
    created, duration, isOutbound, direction) so existing
    count_calls_for_user() works without changes.
    since/until are UTC-aware datetimes.
    """
    if not is_available():
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if until:
                    cur.execute(
                        """
                        SELECT fub_call_id, user_id, person_id, created,
                               duration, is_outbound, direction
                        FROM calls_cache
                        WHERE created >= %s AND created <= %s
                        ORDER BY created DESC
                        """,
                        (since, until),
                    )
                else:
                    cur.execute(
                        """
                        SELECT fub_call_id, user_id, person_id, created,
                               duration, is_outbound, direction
                        FROM calls_cache
                        WHERE created >= %s
                        ORDER BY created DESC
                        """,
                        (since,),
                    )
                rows = cur.fetchall()
        result = []
        for row in rows:
            fub_call_id, user_id, person_id, created, duration, is_outbound, direction = row
            result.append({
                "id":         fub_call_id,
                "userId":     user_id,
                "personId":   person_id,
                "created":    created.isoformat() if hasattr(created, "isoformat") else created,
                "duration":   duration,
                "isOutbound": is_outbound,
                "direction":  direction,
            })
        return result
    except Exception as e:
        logger.warning("get_cached_calls failed: %s", e)
        return []


def get_calls_cache_watermark():
    """Return the most recent 'created' timestamp in calls_cache, or None if empty."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(created) FROM calls_cache")
                row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.warning("get_calls_cache_watermark failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Pond Email Log  (LeadStream email marketing)
# ---------------------------------------------------------------------------

def ensure_pond_email_log_table():
    """Create pond_email_log table if it doesn't exist, and migrate missing columns."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pond_email_log (
                        id              SERIAL PRIMARY KEY,
                        person_id       INTEGER NOT NULL,
                        person_name     VARCHAR(255),
                        email_address   VARCHAR(255),
                        subject         TEXT,
                        strategy        VARCHAR(100),
                        leadstream_tier VARCHAR(50),
                        behavior_summary TEXT,
                        sent_at         TIMESTAMP DEFAULT NOW(),
                        dry_run         BOOLEAN DEFAULT FALSE,
                        sg_message_id   VARCHAR(255),
                        sequence_num    INTEGER DEFAULT 1,
                        UNIQUE(person_id, sent_at)
                    )
                """)
                # Migrate: add sequence_num to existing tables that predate this column
                cur.execute("""
                    ALTER TABLE pond_email_log
                    ADD COLUMN IF NOT EXISTS sequence_num INTEGER DEFAULT 1
                """)
                # Migrate: add avatar_used for A/B tracking of HeyGen avatar variants
                cur.execute("""
                    ALTER TABLE pond_email_log
                    ADD COLUMN IF NOT EXISTS avatar_used VARCHAR(64)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_pond_email_person
                    ON pond_email_log(person_id, sent_at DESC)
                """)
    except Exception as e:
        logger.warning("ensure_pond_email_log_table failed: %s", e)


def log_pond_email(person_id, person_name, email_address, subject,
                   strategy, leadstream_tier, behavior_summary="",
                   dry_run=False, sg_message_id=None, sequence_num=1,
                   avatar_used=None):
    """Record a sent pond email. Returns the inserted row id (for sg_id update), or None."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pond_email_log
                        (person_id, person_name, email_address, subject,
                         strategy, leadstream_tier, behavior_summary,
                         dry_run, sg_message_id, sequence_num, avatar_used)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (person_id, person_name, email_address, subject,
                      strategy, leadstream_tier, behavior_summary,
                      dry_run, sg_message_id, sequence_num, avatar_used))
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.warning("log_pond_email failed for person %s: %s", person_id, e)
        return None


def count_pond_emails_today_by_strategy(strategy, tz_name="America/New_York"):
    """Count real (non-dry-run) pond emails sent today for a specific strategy."""
    if not is_available():
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM pond_email_log
                    WHERE dry_run = FALSE
                      AND strategy = %s
                      AND sent_at >= (NOW() AT TIME ZONE %s)::DATE
                      AND sent_at <  (NOW() AT TIME ZONE %s)::DATE + INTERVAL '1 day'
                """, (strategy, tz_name, tz_name))
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.warning("count_pond_emails_today_by_strategy failed: %s", e)
        return 0


def count_pond_emails_today(tz_name="America/New_York"):
    """Count real (non-dry-run) TEXT pond emails sent today (HeyGen excluded).

    HeyGen video emails are exempt from the main daily cap — they have their
    own ceiling via count_heygen_today(). Excluding them here ensures a full
    day of text sends never blocks a high-value video at first contact.
    """
    if not is_available():
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM pond_email_log
                    WHERE dry_run = FALSE
                      AND avatar_used IS NULL
                      AND sent_at >= (NOW() AT TIME ZONE %s)::DATE
                      AND sent_at <  (NOW() AT TIME ZONE %s)::DATE + INTERVAL '1 day'
                """, (tz_name, tz_name))
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.warning("count_pond_emails_today failed: %s", e)
        return 0


def count_heygen_today(tz_name="America/New_York"):
    """Count real (non-dry-run) HeyGen video emails sent today.

    HeyGen rows have avatar_used set to the avatar ID that rendered the video.
    Used to enforce HEYGEN_DAILY_CAP independently of the text email cap.
    """
    if not is_available():
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM pond_email_log
                    WHERE dry_run = FALSE
                      AND avatar_used IS NOT NULL
                      AND sent_at >= (NOW() AT TIME ZONE %s)::DATE
                      AND sent_at <  (NOW() AT TIME ZONE %s)::DATE + INTERVAL '1 day'
                """, (tz_name, tz_name))
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.warning("count_heygen_today failed: %s", e)
        return 0


def has_received_new_lead_immediate(person_id):
    """Return True if this lead has already received a new_lead_immediate email."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM pond_email_log
                    WHERE person_id = %s AND strategy = 'new_lead_immediate'
                    LIMIT 1
                """, (person_id,))
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("has_received_new_lead_immediate failed for %s: %s", person_id, e)
        return False


def delete_pond_emails_today(tz_name="America/New_York"):
    """Delete today's real (non-dry-run) pond email log entries. Returns count deleted."""
    if not is_available():
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM pond_email_log
                    WHERE dry_run = FALSE
                      AND sent_at >= (NOW() AT TIME ZONE %s)::DATE
                      AND sent_at <  (NOW() AT TIME ZONE %s)::DATE + INTERVAL '1 day'
                """, (tz_name, tz_name))
                return cur.rowcount
    except Exception as e:
        logger.warning("delete_pond_emails_today failed: %s", e)
        return 0


def update_pond_email_sg_id(log_id, sg_message_id):
    """Update the SendGrid message ID on an already-logged pond email row."""
    if not is_available() or not log_id:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pond_email_log SET sg_message_id = %s WHERE id = %s
                """, (sg_message_id, log_id))
    except Exception as e:
        logger.warning("update_pond_email_sg_id failed for log_id %s: %s", log_id, e)


def get_lead_email_history(person_id):
    """
    Return email sequence info for a pond lead.

    Sequence design:
      Phase 1 — emails 1-3:  reply sprint, 3-day cadence
      Phase 2 — emails 4-9:  long-term drip, 15-day cadence (5 gaps × 15d = 75-day drip)
      After email 9:          suppressed (lead is exhausted, ~2.5 months of nurture)

    Returns:
        emails_sent    — count of live (non-dry-run) emails sent
        has_replied    — True if any entry in pond_reply_log
        sequence_num   — which email to send next (1 through 9+)
        suppressed     — True if lead has replied (agent handles), unsubscribed,
                         or has exhausted the full 9-email sequence
    """
    if not is_available():
        return {"emails_sent": 0, "has_replied": False, "sequence_num": 1, "suppressed": False}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), MAX(sent_at)
                    FROM pond_email_log
                    WHERE person_id = %s AND dry_run = FALSE
                      AND strategy != 'new_lead_immediate'
                """, (person_id,))
                row = cur.fetchone()
                emails_sent = row[0] if row else 0
                last_sent   = row[1] if row else None

                cur.execute("""
                    SELECT COUNT(*) FROM pond_reply_log WHERE person_id = %s
                """, (person_id,))
                row = cur.fetchone()
                has_replied = (row[0] > 0) if row else False

        # Suppressed only after full 9-email sequence (≈2.5 months of nurture)
        # Cooldown pacing (3d vs 15d) is enforced in pond_mailer.py, not here.
        MAX_SEQUENCE = 9
        suppressed = emails_sent >= MAX_SEQUENCE and not has_replied

        # Cap at MAX_SEQUENCE when suppressed — returning 10+ would be misleading
        # since the lead is in quiet period and no new email will be sent.
        sequence_num = min(emails_sent + 1, MAX_SEQUENCE) if suppressed else emails_sent + 1

        return {
            "emails_sent":   emails_sent,
            "has_replied":   has_replied,
            "sequence_num":  sequence_num,
            "suppressed":    suppressed,
        }
    except Exception as e:
        logger.warning("get_lead_email_history failed for person %s: %s", person_id, e)
        return {"emails_sent": 0, "has_replied": False, "sequence_num": 1, "suppressed": False}


def get_immediate_email_context(person_id):
    """
    Return the time bucket of the most recent new_lead_immediate email for this person.
    Used by pond_mailer's HeyGen blocks to reference the overnight/early text in the
    morning video wrapper.

    Returns: "late_night" (11pm–4am ET), "early_morning" (4am–7am ET), or None (normal hours / no email).
    """
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sent_at FROM pond_email_log
                    WHERE person_id = %s AND strategy = 'new_lead_immediate'
                      AND dry_run = FALSE
                    ORDER BY sent_at DESC LIMIT 1
                """, (person_id,))
                row = cur.fetchone()
        if row and row[0]:
            from datetime import timezone
            from zoneinfo import ZoneInfo
            sent_utc = row[0].replace(tzinfo=timezone.utc)
            et_hour  = sent_utc.astimezone(ZoneInfo("America/New_York")).hour
            if et_hour >= 23 or et_hour < 4:
                return "late_night"
            if 4 <= et_hour < 7:
                return "early_morning"
        return None
    except Exception as e:
        logger.warning("get_immediate_email_context failed for %s: %s", person_id, e)
        return None


def days_since_last_pond_email(person_id):
    """Return days since last email to this lead, or None if never emailed."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(sent_at) FROM pond_email_log
                    WHERE person_id = %s AND dry_run = FALSE
                      AND strategy != 'new_lead_immediate'
                """, (person_id,))
                row = cur.fetchone()
        if row and row[0]:
            from datetime import datetime, timezone
            last = row[0].replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - last
            return delta.total_seconds() / 86400
        return None
    except Exception as e:
        logger.warning("days_since_last_pond_email failed: %s", e)
        return None


def ensure_pond_reply_log_table():
    """Create pond_reply_log table if it doesn't exist."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pond_reply_log (
                        id              SERIAL PRIMARY KEY,
                        person_id       INTEGER,
                        person_name     VARCHAR(255),
                        reply_from      VARCHAR(255),
                        reply_text      TEXT,
                        sentiment       VARCHAR(20),
                        sentiment_score FLOAT,
                        routed          BOOLEAN DEFAULT FALSE,
                        fub_task_id     INTEGER,
                        received_at     TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_pond_reply_person
                    ON pond_reply_log(person_id, received_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_pond_reply_from
                    ON pond_reply_log(reply_from)
                """)
    except Exception as e:
        logger.warning("ensure_pond_reply_log_table failed: %s", e)


def get_pond_email_person_by_email(email_address):
    """Find the most recently emailed pond lead matching this email address.
    Returns (person_id, person_name, sequence_num) or (None, None, None).
    sequence_num is the email number that most likely triggered the reply.
    """
    if not is_available():
        return None, None, None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, person_name, sequence_num
                    FROM pond_email_log
                    WHERE LOWER(email_address) = LOWER(%s)
                      AND dry_run = FALSE
                    ORDER BY sent_at DESC
                    LIMIT 1
                """, (email_address,))
                row = cur.fetchone()
        if row:
            return row[0], row[1], row[2]
        return None, None, None
    except Exception as e:
        logger.warning("get_pond_email_person_by_email failed: %s", e)
        return None, None, None


def log_pond_reply(person_id, person_name, reply_from, reply_text,
                   sentiment, sentiment_score, routed=False, fub_task_id=None):
    """Record an inbound reply to a pond email."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pond_reply_log
                        (person_id, person_name, reply_from, reply_text,
                         sentiment, sentiment_score, routed, fub_task_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (person_id, person_name, reply_from, reply_text,
                      sentiment, sentiment_score, routed, fub_task_id))
    except Exception as e:
        logger.warning("log_pond_reply failed for person %s: %s", person_id, e)


def get_pond_reply_stats(days=30):
    """Reply stats for monitoring — how many replies, how many converted."""
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                     AS total_replies,
                        COUNT(*) FILTER (WHERE sentiment='positive') AS positive,
                        COUNT(*) FILTER (WHERE sentiment='neutral')  AS neutral,
                        COUNT(*) FILTER (WHERE sentiment='negative') AS negative,
                        COUNT(*) FILTER (WHERE routed=TRUE)          AS routed
                    FROM pond_reply_log
                    WHERE received_at >= NOW() - INTERVAL '%s days'
                """ % days)
                row = cur.fetchone()
        if row:
            return {
                "total_replies": row[0],
                "positive":      row[1],
                "neutral":       row[2],
                "negative":      row[3],
                "routed":        row[4],
            }
        return {}
    except Exception as e:
        logger.warning("get_pond_reply_stats failed: %s", e)
        return {}


def get_pond_dashboard_data(days=30):
    """All data needed for the AI Outreach dashboard tab."""
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # ── Funnel ──────────────────────────────────────────────────
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE NOT dry_run)                     AS sent,
                        COUNT(DISTINCT person_id) FILTER (WHERE NOT dry_run)    AS unique_leads
                    FROM pond_email_log
                    WHERE sent_at >= NOW() - INTERVAL '%s days'
                """ % days)
                row = cur.fetchone()
                sent         = row[0] if row else 0
                unique_leads = row[1] if row else 0

                cur.execute("""
                    SELECT
                        COUNT(*)                                     AS replies,
                        COUNT(*) FILTER (WHERE sentiment='positive') AS positive,
                        COUNT(*) FILTER (WHERE sentiment='neutral')  AS neutral,
                        COUNT(*) FILTER (WHERE sentiment='negative') AS negative,
                        COUNT(*) FILTER (WHERE routed=TRUE)          AS routed
                    FROM pond_reply_log
                    WHERE received_at >= NOW() - INTERVAL '%s days'
                """ % days)
                row = cur.fetchone()
                replies  = row[0] if row else 0
                positive = row[1] if row else 0
                neutral  = row[2] if row else 0
                negative = row[3] if row else 0
                routed   = row[4] if row else 0

                # ── Daily sends — last 14 days ───────────────────────────
                cur.execute("""
                    SELECT DATE(sent_at AT TIME ZONE 'America/New_York') AS day,
                           COUNT(*) AS cnt
                    FROM pond_email_log
                    WHERE NOT dry_run
                      AND sent_at >= NOW() - INTERVAL '14 days'
                    GROUP BY 1 ORDER BY 1
                """)
                daily_map = {str(r[0]): r[1] for r in cur.fetchall()}

                # ── Routed leads (enriched by caller) ────────────────────
                cur.execute("""
                    SELECT person_id, person_name, reply_from,
                           reply_text, sentiment, received_at, fub_task_id
                    FROM pond_reply_log
                    WHERE routed = TRUE
                      AND received_at >= NOW() - INTERVAL '%s days'
                    ORDER BY received_at DESC
                    LIMIT 40
                """ % days)
                routed_rows = cur.fetchall()

                # ── Recent replies feed ───────────────────────────────────
                cur.execute("""
                    SELECT person_id, person_name, reply_from,
                           reply_text, sentiment, received_at, routed
                    FROM pond_reply_log
                    WHERE received_at >= NOW() - INTERVAL '%s days'
                    ORDER BY received_at DESC
                    LIMIT 25
                """ % days)
                reply_rows = cur.fetchall()

        # ── Build daily chart (fill gaps with 0) ─────────────────────────
        today = date.today()
        daily_chart = []
        for i in range(13, -1, -1):
            d = today - timedelta(days=i)
            daily_chart.append({
                "date":  str(d),
                "label": d.strftime("%-m/%-d"),
                "count": daily_map.get(str(d), 0),
            })

        routed_leads = [
            {
                "person_id":   r[0],
                "person_name": r[1] or r[2] or "Unknown",
                "reply_from":  r[2],
                "reply_text":  (r[3] or "")[:280],
                "sentiment":   r[4],
                "received_at": r[5].isoformat() if r[5] else None,
                "received_ts": r[5].timestamp() if r[5] else 0,
                "fub_task_id": r[6],
            }
            for r in routed_rows
        ]

        recent_replies = [
            {
                "person_id":   r[0],
                "person_name": r[1] or r[2] or "Unknown",
                "reply_text":  (r[3] or "")[:200],
                "sentiment":   r[4],
                "received_at": r[5].isoformat() if r[5] else None,
                "routed":      r[6],
            }
            for r in reply_rows
        ]

        return {
            "funnel": {
                "sent":     sent,
                "unique":   unique_leads,
                "replied":  replies,
                "positive": positive,
                "neutral":  neutral,
                "negative": negative,
                "routed":   routed,
            },
            "daily_chart":    daily_chart,
            "routed_leads":   routed_leads,
            "recent_replies": recent_replies,
        }
    except Exception as e:
        logger.warning("get_pond_dashboard_data failed: %s", e)
        return {}


def get_pond_email_stats(days=30):
    """Summary stats for the email log — for dashboard/monitoring."""
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # True totals — no GROUP BY so we get the real aggregate
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE NOT dry_run)                    AS total_sent,
                        COUNT(DISTINCT person_id) FILTER (WHERE NOT dry_run)   AS unique_leads
                    FROM pond_email_log
                    WHERE sent_at >= NOW() - INTERVAL '%s days'
                """ % days)
                totals = cur.fetchone() or (0, 0)

                # Per-strategy breakdown
                cur.execute("""
                    SELECT strategy, COUNT(*) AS cnt
                    FROM pond_email_log
                    WHERE sent_at >= NOW() - INTERVAL '%s days'
                    GROUP BY strategy
                    ORDER BY cnt DESC
                """ % days)
                rows = cur.fetchall()

        return {
            "by_strategy": [
                {"strategy": r[0], "count": r[1]} for r in rows
            ],
            "total_sent": totals[0],
            "unique_leads": totals[1],
        }
    except Exception as e:
        logger.warning("get_pond_email_stats failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Pond Mailer Job Tracking (DB-backed so jobs survive Railway redeploys)
# ---------------------------------------------------------------------------

def ensure_pond_mailer_jobs_table():
    """Create pond_mailer_jobs table if it doesn't exist."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pond_mailer_jobs (
                        job_id      VARCHAR(16) PRIMARY KEY,
                        status      VARCHAR(20) NOT NULL DEFAULT 'running',
                        dry_run     BOOLEAN NOT NULL DEFAULT FALSE,
                        result      JSONB,
                        error       TEXT,
                        started_at  TIMESTAMP DEFAULT NOW(),
                        finished_at TIMESTAMP
                    )
                """)
            conn.commit()
    except Exception as e:
        logger.warning("ensure_pond_mailer_jobs_table failed: %s", e)


def create_pond_mailer_job(job_id, dry_run=False):
    """Insert a new running job record."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pond_mailer_jobs (job_id, status, dry_run)
                    VALUES (%s, 'running', %s)
                    ON CONFLICT (job_id) DO NOTHING
                """, (job_id, dry_run))
            conn.commit()
    except Exception as e:
        logger.warning("create_pond_mailer_job failed: %s", e)


def finish_pond_mailer_job(job_id, result=None, error=None):
    """Mark a job complete or errored, storing its result."""
    if not is_available():
        return
    import json as _json
    status = "error" if error else "complete"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pond_mailer_jobs
                    SET status = %s, result = %s, error = %s, finished_at = NOW()
                    WHERE job_id = %s
                """, (status, _json.dumps(result) if result else None, error, job_id))
            conn.commit()
    except Exception as e:
        logger.warning("finish_pond_mailer_job failed: %s", e)


def timeout_stale_pond_jobs(max_minutes=30):
    """Mark any job stuck in 'running' for longer than max_minutes as 'timeout'.

    Called at the start of each run and when job status is polled, so zombie
    jobs (killed mid-run by a Railway redeploy) auto-resolve instead of
    staying 'running' forever in the dashboard.
    """
    if not is_available():
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pond_mailer_jobs
                    SET status = 'timeout',
                        error  = 'Job exceeded max runtime — likely killed by a Railway redeploy.',
                        finished_at = NOW()
                    WHERE status = 'running'
                      AND started_at < NOW() - INTERVAL '%s minutes'
                    RETURNING job_id
                """, (max_minutes,))
                timed_out = [r[0] for r in cur.fetchall()]
            conn.commit()
        if timed_out:
            logger.info("Timed out stale pond jobs: %s", timed_out)
        return len(timed_out)
    except Exception as e:
        logger.warning("timeout_stale_pond_jobs failed: %s", e)
        return 0


def get_pond_mailer_job(job_id):
    """Fetch job status and result by ID. Returns None if not found.

    Auto-resolves stale running jobs before returning so the dashboard
    never shows a zombie job.
    """
    if not is_available():
        return None
    try:
        # Resolve any zombie jobs first
        timeout_stale_pond_jobs()

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT job_id, status, dry_run, result, error, started_at, finished_at
                    FROM pond_mailer_jobs
                    WHERE job_id = %s
                """, (job_id,))
                row = cur.fetchone()
        if not row:
            return None
        job_id_, status, dry_run, result, error, started_at, finished_at = row
        out = {"job_id": job_id_, "status": status, "dry_run": dry_run}
        if result:
            out.update(result if isinstance(result, dict) else {})
        if error:
            out["error"] = error
        return out
    except Exception as e:
        logger.warning("get_pond_mailer_job failed: %s", e)
        return None


def get_pond_recent_activity():
    """
    Return a dashboard-friendly snapshot of pond mailer activity.
    Reads from pond_email_log (always reliable) + pond_mailer_jobs (best-effort).
    Called by /api/pond-mailer/recent so Barry can check status without Railway access.
    """
    from datetime import datetime, timezone, timedelta
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now = datetime.now(timezone.utc)
    now_et = now.astimezone(ET)
    today_str = now_et.strftime("%Y-%m-%d")

    out = {
        "as_of": now_et.strftime("%Y-%m-%d %I:%M %p ET"),
        "db_available": is_available(),
        "today": {},
        "last_7_days": {},
        "recent_sends": [],
        "recent_jobs": [],
        "errors": [],
    }

    if not is_available():
        out["errors"].append("DATABASE_URL not set — no DB access")
        return out

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # Today's sends (ET date)
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE strategy != 'new_lead_immediate') AS sequence_emails,
                        COUNT(*) FILTER (WHERE strategy = 'new_lead_immediate')  AS immediate_texts,
                        COUNT(*) FILTER (WHERE dry_run = TRUE)                   AS dry_runs
                    FROM pond_email_log
                    WHERE sent_at AT TIME ZONE 'America/New_York' >= %s::date
                """, (today_str,))
                row = cur.fetchone()
                out["today"] = {
                    "sequence_emails": row[0],
                    "immediate_texts": row[1],
                    "dry_runs":        row[2],
                }

                # Last 7 days
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE strategy != 'new_lead_immediate' AND dry_run = FALSE),
                        COUNT(*) FILTER (WHERE strategy = 'new_lead_immediate'  AND dry_run = FALSE),
                        COUNT(DISTINCT person_id) FILTER (WHERE dry_run = FALSE)
                    FROM pond_email_log
                    WHERE sent_at > NOW() - INTERVAL '7 days'
                """)
                row = cur.fetchone()
                out["last_7_days"] = {
                    "sequence_emails": row[0],
                    "immediate_texts": row[1],
                    "unique_leads":    row[2],
                }

                # Most recent 20 sends
                cur.execute("""
                    SELECT person_id, person_name, subject, strategy, sequence_num,
                           dry_run, sent_at AT TIME ZONE 'America/New_York' AS sent_et
                    FROM pond_email_log
                    ORDER BY sent_at DESC
                    LIMIT 20
                """)
                rows = cur.fetchall()
                out["recent_sends"] = [
                    {
                        "person_id":    r[0],
                        "name":         r[1],
                        "subject":      r[2],
                        "strategy":     r[3],
                        "sequence_num": r[4],
                        "dry_run":      r[5],
                        "sent_et":      r[6].strftime("%m/%d %I:%M %p") if r[6] else None,
                    }
                    for r in rows
                ]

                # Recent jobs from pond_mailer_jobs (may not exist yet)
                try:
                    cur.execute("""
                        SELECT job_id, status, dry_run, error,
                               started_at AT TIME ZONE 'America/New_York',
                               finished_at AT TIME ZONE 'America/New_York'
                        FROM pond_mailer_jobs
                        ORDER BY started_at DESC
                        LIMIT 10
                    """)
                    job_rows = cur.fetchall()
                    out["recent_jobs"] = [
                        {
                            "job_id":      r[0],
                            "status":      r[1],
                            "dry_run":     r[2],
                            "error":       (r[3] or "")[:200] if r[3] else None,
                            "started_et":  r[4].strftime("%m/%d %I:%M %p") if r[4] else None,
                            "finished_et": r[5].strftime("%m/%d %I:%M %p") if r[5] else None,
                        }
                        for r in job_rows
                    ]
                except Exception as je:
                    out["recent_jobs"] = []
                    out["errors"].append(f"pond_mailer_jobs table not yet created: {je}")

    except Exception as e:
        out["errors"].append(f"DB query failed: {e}")

    return out


# ===========================================================================
# SERENDIPITY CLAUSE — Behavioral trigger email system
# ===========================================================================

def ensure_serendipity_tables():
    """Create serendipity_log and serendipity_cursor tables if they don't exist."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS serendipity_log (
                        id              SERIAL PRIMARY KEY,
                        person_id       INTEGER NOT NULL,
                        person_name     TEXT,
                        email_address   TEXT,
                        trigger_type    TEXT NOT NULL,
                        trigger_data    JSONB,
                        fire_after      TIMESTAMPTZ NOT NULL,
                        status          TEXT NOT NULL DEFAULT 'pending',
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sent_at         TIMESTAMPTZ,
                        sg_message_id   TEXT,
                        skip_reason     TEXT,
                        dry_run         BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_seren_pending
                    ON serendipity_log(status, fire_after)
                    WHERE status = 'pending'
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_seren_person
                    ON serendipity_log(person_id, created_at DESC)
                """)
                # Cursor table — single row, tracks the last event scan timestamp
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS serendipity_cursor (
                        id              INTEGER PRIMARY KEY DEFAULT 1,
                        last_checked_at TIMESTAMPTZ NOT NULL,
                        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
    except Exception as e:
        logger.warning("ensure_serendipity_tables failed: %s", e)


def get_serendipity_cursor():
    """Return the timestamp of the last serendipity event scan, or None."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT last_checked_at FROM serendipity_cursor WHERE id = 1")
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.warning("get_serendipity_cursor failed: %s", e)
        return None


def set_serendipity_cursor(ts):
    """Upsert the serendipity event scan cursor."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO serendipity_cursor (id, last_checked_at, updated_at)
                    VALUES (1, %s, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET last_checked_at = EXCLUDED.last_checked_at,
                            updated_at      = NOW()
                """, (ts,))
    except Exception as e:
        logger.warning("set_serendipity_cursor failed: %s", e)


def log_serendipity_trigger(person_id, person_name, email_address,
                             trigger_type, trigger_data, fire_after,
                             dry_run=False):
    """Insert a pending serendipity trigger. Returns the new row id, or None."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO serendipity_log
                        (person_id, person_name, email_address,
                         trigger_type, trigger_data, fire_after, dry_run)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (person_id, person_name, email_address,
                      trigger_type, json.dumps(trigger_data), fire_after, dry_run))
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.warning("log_serendipity_trigger failed for person %s: %s", person_id, e)
        return None


def has_pending_serendipity_trigger(person_id, trigger_type, address=None):
    """
    True if a pending (or recently queued) trigger already exists for this
    person + type + address combo. Prevents double-queuing the same event.
    Window: look back 24 hours to catch triggers queued but not yet fired.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if address:
                    cur.execute("""
                        SELECT 1 FROM serendipity_log
                        WHERE person_id   = %s
                          AND trigger_type = %s
                          AND status IN ('pending', 'sent')
                          AND trigger_data->>'address' = %s
                          AND created_at > NOW() - INTERVAL '7 days'
                        LIMIT 1
                    """, (person_id, trigger_type, address))
                else:
                    cur.execute("""
                        SELECT 1 FROM serendipity_log
                        WHERE person_id   = %s
                          AND trigger_type = %s
                          AND status IN ('pending', 'sent')
                          AND created_at > NOW() - INTERVAL '7 days'
                        LIMIT 1
                    """, (person_id, trigger_type))
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("has_pending_serendipity_trigger failed: %s", e)
        return False


def get_pending_serendipity_triggers(now=None):
    """Return pending triggers whose fire_after has passed. Ordered oldest first."""
    if not is_available():
        return []
    try:
        ts = now or datetime.now(timezone.utc)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, person_id, person_name, email_address,
                           trigger_type, trigger_data, fire_after, dry_run
                    FROM serendipity_log
                    WHERE status = 'pending' AND fire_after <= %s
                    ORDER BY fire_after ASC
                    LIMIT 20
                """, (ts,))
                rows = cur.fetchall()
        return [
            {
                "id":           r[0],
                "person_id":    r[1],
                "person_name":  r[2],
                "email_address":r[3],
                "trigger_type": r[4],
                "trigger_data": r[5] or {},
                "fire_after":   r[6],
                "dry_run":      r[7],
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("get_pending_serendipity_triggers failed: %s", e)
        return []


def mark_serendipity_sent(trigger_id, sg_message_id=None):
    """Mark a trigger as sent."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE serendipity_log
                    SET status = 'sent', sent_at = NOW(), sg_message_id = %s
                    WHERE id = %s
                """, (sg_message_id, trigger_id))
    except Exception as e:
        logger.warning("mark_serendipity_sent failed: %s", e)


def mark_serendipity_skipped(trigger_id, reason=None):
    """Mark a trigger as skipped with an optional reason."""
    if not is_available():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE serendipity_log
                    SET status = 'skipped', skip_reason = %s
                    WHERE id = %s
                """, (reason, trigger_id))
    except Exception as e:
        logger.warning("mark_serendipity_skipped failed: %s", e)


def has_recent_serendipity_sent(person_id, hours=168):
    """True if a serendipity email was sent to this lead within the last `hours`."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM serendipity_log
                    WHERE person_id = %s
                      AND status = 'sent'
                      AND sent_at > NOW() - INTERVAL '%s hours'
                    LIMIT 1
                """, (person_id, hours))
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("has_recent_serendipity_sent failed: %s", e)
        return False


def has_any_recent_email(person_id, hours=48):
    """
    True if ANY email (drip OR serendipity) was sent to this lead in the last
    `hours`. Used to prevent email collision between the two systems.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check pond drip emails
                cur.execute("""
                    SELECT 1 FROM pond_email_log
                    WHERE person_id = %s
                      AND dry_run   = FALSE
                      AND sent_at   > NOW() - (%s || ' hours')::INTERVAL
                    LIMIT 1
                """, (person_id, str(hours)))
                if cur.fetchone():
                    return True
                # Check serendipity emails
                cur.execute("""
                    SELECT 1 FROM serendipity_log
                    WHERE person_id = %s
                      AND status    = 'sent'
                      AND sent_at   > NOW() - (%s || ' hours')::INTERVAL
                    LIMIT 1
                """, (person_id, str(hours)))
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("has_any_recent_email failed: %s", e)
        return False


def get_serendipity_stats(days=30):
    """Summary stats for the Serendipity Clause dashboard."""
    if not is_available():
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                           AS total,
                        COUNT(*) FILTER (WHERE status = 'sent')           AS sent,
                        COUNT(*) FILTER (WHERE status = 'pending')        AS pending,
                        COUNT(*) FILTER (WHERE status = 'skipped')        AS skipped,
                        COUNT(*) FILTER (WHERE trigger_type = 'saved_property'   AND status = 'sent') AS saved_sent,
                        COUNT(*) FILTER (WHERE trigger_type = 'repeat_view'      AND status = 'sent') AS repeat_sent,
                        COUNT(*) FILTER (WHERE trigger_type = 'inactivity_return' AND status = 'sent') AS return_sent
                    FROM serendipity_log
                    WHERE created_at > NOW() - (%s || ' days')::INTERVAL
                      AND dry_run = FALSE
                """, (str(days),))
                row = cur.fetchone()
                return {
                    "total":        row[0],
                    "sent":         row[1],
                    "pending":      row[2],
                    "skipped":      row[3],
                    "by_trigger": {
                        "saved_property":    row[4],
                        "repeat_view":       row[5],
                        "inactivity_return": row[6],
                    },
                    "days":         days,
                }
    except Exception as e:
        logger.warning("get_serendipity_stats failed: %s", e)


# ---------------------------------------------------------------------------
# Cross-worker API cache  (survives Railway restarts + gunicorn worker misses)
# ---------------------------------------------------------------------------

def db_cache_set(cache_key: str, data: dict) -> bool:
    """
    Persist a JSON payload to api_cache under cache_key.
    Used by app.py's cache_set() to share data across gunicorn workers
    and survive Railway restarts.
    Returns True on success.
    """
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO api_cache (cache_key, cached_at, day_key, data)
                    VALUES (%s, NOW(), CURRENT_DATE, %s)
                    ON CONFLICT (cache_key) DO UPDATE SET
                        cached_at = NOW(),
                        day_key   = CURRENT_DATE,
                        data      = EXCLUDED.data
                """, (cache_key, json.dumps(data)))
        return True
    except Exception as e:
        logger.warning("db_cache_set(%s) failed: %s", cache_key, e)
        return False


def db_cache_get(cache_key: str, max_age_hours: int = 24):
    """
    Retrieve a JSON payload from api_cache.
    Returns None if the key doesn't exist or is older than max_age_hours.
    """
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT data, cached_at
                    FROM api_cache
                    WHERE cache_key = %s
                      AND cached_at >= NOW() - (%s || ' hours')::INTERVAL
                """, (cache_key, str(max_age_hours)))
                row = cur.fetchone()
        if row:
            data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return data
        return None
    except Exception as e:
        logger.warning("db_cache_get(%s) failed: %s", cache_key, e)
        return None
        return {}
