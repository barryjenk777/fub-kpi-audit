"""
Database layer for Legacy Home Team KPI Audit.

Provides Postgres persistence (via DATABASE_URL) with graceful fallback to
file-based storage when no database is configured. All public functions are
safe to call — they silently return None / empty structures if the DB is
unavailable.

Tables
------
engagement_runs   — one row per agent per scoring run (replaces engagement_log.json)
leadstream_manifest — single-row JSONB store for the scoring manifest
goals             — agent goals per metric/period (new for goal-setting feature)
"""

import os
import json
import logging
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
        # Railway gives postgres:// but psycopg2 needs postgresql://
        if _db_url.startswith("postgres://"):
            _db_url = "postgresql://" + _db_url[len("postgres://"):]
    return _db_url


def is_available():
    """Return True if DATABASE_URL is set."""
    return bool(_get_url())


@contextmanager
def get_conn():
    """Context manager — yields a psycopg2 connection or raises."""
    import psycopg2
    import psycopg2.extras
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
# Schema bootstrap
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

CREATE INDEX IF NOT EXISTS idx_er_day_et
    ON engagement_runs (day_et DESC);

CREATE INDEX IF NOT EXISTS idx_er_agent
    ON engagement_runs (agent_name, day_et DESC);

CREATE TABLE IF NOT EXISTS leadstream_manifest (
    id         SERIAL PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data       JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS goals (
    id             SERIAL  PRIMARY KEY,
    agent_name     TEXT    NOT NULL,
    metric         TEXT    NOT NULL,
    target         INTEGER NOT NULL,
    period         TEXT    NOT NULL DEFAULT 'weekly',
    effective_from DATE    NOT NULL DEFAULT CURRENT_DATE,
    effective_to   DATE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes          TEXT,
    UNIQUE (agent_name, metric, effective_from)
);
"""


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
        logger.info("Database schema verified / initialized.")
        return True
    except Exception as e:
        logger.warning("DB init failed (continuing with file storage): %s", e)
        return False


# ---------------------------------------------------------------------------
# Engagement log helpers
# ---------------------------------------------------------------------------

def _et_offset():
    """Return UTC offset hours for US Eastern time (-4 EDT, -5 EST)."""
    m = datetime.now(timezone.utc).month
    return -4 if 3 <= m <= 10 else -5


def write_engagement_entries(run_time_iso: str, mode: str,
                              agents: dict, pond: dict):
    """
    Upsert one row per agent + one pond row for a scoring run.

    agents = {"Agent Name": {"tagged": N, "actioned": N}, ...}
    pond   = {"tagged": N, "actioned": N}
    """
    if not is_available():
        return False
    try:
        ET = timezone(timedelta(hours=_et_offset()))
        run_time = datetime.fromisoformat(run_time_iso.replace("Z", "+00:00"))
        day_et = (run_time.astimezone(ET)).date()

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
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_time, agent_name, is_pond)
                        DO UPDATE SET
                            tagged   = GREATEST(engagement_runs.tagged,   EXCLUDED.tagged),
                            actioned = GREATEST(engagement_runs.actioned, EXCLUDED.actioned),
                            mode     = EXCLUDED.mode,
                            captured_at = NOW()
                    """, row)
        return True
    except Exception as e:
        logger.warning("write_engagement_entries failed: %s", e)
        return False


def read_engagement_log(days: int = 7) -> dict:
    """
    Return engagement data in the same dict structure the weekly endpoint
    expects: {run_time_iso: {"mode": ..., "agents": {...}, "pond": {...}}}

    Aggregates by (run_time, agent) taking MAX(tagged, actioned).
    """
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


def write_engagement_from_log_dict(eng_log: dict):
    """
    Bulk-import an existing engagement_log.json dict into the DB.
    Used during migration / backfill.
    """
    if not is_available():
        return 0
    count = 0
    for run_time_iso, rec in eng_log.items():
        ok = write_engagement_entries(
            run_time_iso,
            rec.get("mode", "full"),
            rec.get("agents", {}),
            rec.get("pond", {}),
        )
        if ok:
            count += 1
    return count


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def write_manifest(manifest: dict):
    """Store the full manifest JSON. Replaces whatever was there before."""
    if not is_available():
        return False
    try:
        import psycopg2.extras
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM leadstream_manifest")
                cur.execute(
                    "INSERT INTO leadstream_manifest (data) VALUES (%s)",
                    (json.dumps(manifest),)
                )
        return True
    except Exception as e:
        logger.warning("write_manifest failed: %s", e)
        return False


def read_manifest() -> dict | None:
    """Return the most recent manifest, or None if none exists."""
    if not is_available():
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data FROM leadstream_manifest ORDER BY updated_at DESC LIMIT 1"
                )
                row = cur.fetchone()
        if row:
            return row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return None
    except Exception as e:
        logger.warning("read_manifest failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Goals helpers
# ---------------------------------------------------------------------------

def get_goals(agent_name: str = None, as_of: date = None) -> list[dict]:
    """
    Return active goals. Optionally filter by agent.
    'Active' means effective_from <= as_of AND (effective_to IS NULL OR effective_to >= as_of).
    """
    if not is_available():
        return []
    if as_of is None:
        as_of = date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if agent_name:
                    cur.execute("""
                        SELECT id, agent_name, metric, target, period,
                               effective_from, effective_to, notes
                        FROM   goals
                        WHERE  agent_name = %s
                          AND  effective_from <= %s
                          AND  (effective_to IS NULL OR effective_to >= %s)
                        ORDER  BY agent_name, metric
                    """, (agent_name, as_of, as_of))
                else:
                    cur.execute("""
                        SELECT id, agent_name, metric, target, period,
                               effective_from, effective_to, notes
                        FROM   goals
                        WHERE  effective_from <= %s
                          AND  (effective_to IS NULL OR effective_to >= %s)
                        ORDER  BY agent_name, metric
                    """, (as_of, as_of))
                rows = cur.fetchall()
        cols = ["id", "agent_name", "metric", "target", "period",
                "effective_from", "effective_to", "notes"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.warning("get_goals failed: %s", e)
        return []


def upsert_goal(agent_name: str, metric: str, target: int,
                period: str = "weekly", effective_from: date = None,
                effective_to: date = None, notes: str = None) -> dict | None:
    """
    Insert or update a goal. Returns the goal row as a dict.
    Metrics: 'calls', 'conversations', 'leadstream_actioned', 'leadstream_rate',
             'appointments', 'closings'
    """
    if not is_available():
        return None
    if effective_from is None:
        effective_from = date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO goals
                        (agent_name, metric, target, period, effective_from, effective_to, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (agent_name, metric, effective_from)
                    DO UPDATE SET
                        target       = EXCLUDED.target,
                        period       = EXCLUDED.period,
                        effective_to = EXCLUDED.effective_to,
                        notes        = EXCLUDED.notes
                    RETURNING id, agent_name, metric, target, period,
                              effective_from, effective_to, notes
                """, (agent_name, metric, target, period,
                      effective_from, effective_to, notes))
                row = cur.fetchone()
        if row:
            cols = ["id", "agent_name", "metric", "target", "period",
                    "effective_from", "effective_to", "notes"]
            return dict(zip(cols, row))
        return None
    except Exception as e:
        logger.warning("upsert_goal failed: %s", e)
        return None


def delete_goal(goal_id: int) -> bool:
    if not is_available():
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM goals WHERE id = %s", (goal_id,))
        return True
    except Exception as e:
        logger.warning("delete_goal failed: %s", e)
        return False


def get_goal_progress(week_start: date = None) -> list[dict]:
    """
    Cross-reference active goals with actual engagement_runs data for the
    current (or specified) week. Returns progress % for each agent/metric.

    week_start defaults to the most recent Monday.
    """
    if not is_available():
        return []
    if week_start is None:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())  # Monday
    week_end = week_start + timedelta(days=6)

    goals = get_goals(as_of=week_start)
    if not goals:
        return []

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Pull actuals for this week
                cur.execute("""
                    SELECT agent_name, is_pond,
                           MAX(tagged)   AS tagged,
                           SUM(actioned) AS actioned
                    FROM   engagement_runs
                    WHERE  day_et BETWEEN %s AND %s
                      AND  NOT is_pond
                    GROUP  BY agent_name, is_pond
                """, (week_start, week_end))
                actuals_rows = cur.fetchall()

        actuals = {}  # agent_name -> {tagged, actioned}
        for agent_name, _, tagged, actioned in actuals_rows:
            actuals[agent_name] = {"tagged": tagged or 0, "actioned": actioned or 0}

        progress = []
        for goal in goals:
            agent = goal["agent_name"]
            metric = goal["metric"]
            target = goal["target"]
            actual_val = 0

            if metric == "leadstream_actioned":
                actual_val = actuals.get(agent, {}).get("actioned", 0)
            elif metric == "leadstream_rate":
                a = actuals.get(agent, {})
                t = a.get("tagged", 0)
                actual_val = round(a.get("actioned", 0) / t * 100) if t > 0 else 0

            pct = round(actual_val / target * 100) if target > 0 else 0
            progress.append({
                **goal,
                "actual": actual_val,
                "pct": min(pct, 100),
                "on_track": pct >= 70,
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
            })

        return progress
    except Exception as e:
        logger.warning("get_goal_progress failed: %s", e)
        return []
