"""
database.py — SQLite job state persistence.

Job lifecycle:
  pending → running → done
                    ↘ error
                    ↘ timeout

Schema:
  jobs(id TEXT PK, status TEXT, area TEXT, trade_type TEXT,
       progress_current INT, progress_total INT,
       results JSON, error_message TEXT,
       created_at TEXT, updated_at TEXT)
"""

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from config import settings

# Valid job statuses
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_ERROR = "error"
STATUS_TIMEOUT = "timeout"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _get_conn():
    """Context manager that yields a connection and always closes it."""
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if they don't exist. Safe to call multiple times.

    Also cleans up any jobs that were left in 'pending' or 'running' state
    from a previous server run. Those jobs will never complete, so they are
    marked as errors so the user sees a clear message instead of an infinite
    spinner.
    """
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id              TEXT PRIMARY KEY,
                status          TEXT NOT NULL DEFAULT 'pending',
                area            TEXT NOT NULL,
                trade_type      TEXT NOT NULL,
                progress_current INTEGER NOT NULL DEFAULT 0,
                progress_total   INTEGER NOT NULL DEFAULT 0,
                results         TEXT,
                error_message   TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS priced_services (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id          TEXT NOT NULL,
                business_name   TEXT NOT NULL,
                service_name    TEXT NOT NULL,
                price_value     REAL NOT NULL,
                price_unit      TEXT NOT NULL DEFAULT '',
                source          TEXT NOT NULL DEFAULT 'checkatrade',
                created_at      TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id),
                UNIQUE(job_id, business_name, service_name)
            )
        """
        )
        # Mark orphaned jobs from a prior server run — they will never complete.
        conn.execute(
            """
            UPDATE jobs
            SET status = ?,
                error_message = 'Server restarted — scan did not complete. Please try again.',
                updated_at = ?
            WHERE status IN ('pending', 'running')
            """,
            (STATUS_ERROR, _now()),
        )


def create_job(area: str, trade_type: str) -> str:
    """Insert a new job and return its ID."""
    job_id = str(uuid.uuid4())
    now = _now()
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO jobs
              (id, status, area, trade_type, progress_current, progress_total,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, 0, ?, ?)
        """,
            (job_id, STATUS_PENDING, area, trade_type, now, now),
        )
    return job_id


def get_job(job_id: str) -> Optional[dict]:
    """Return job as dict or None if not found."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
    if row is None:
        return None
    result = dict(row)
    if result.get("results"):
        result["results"] = json.loads(result["results"])
    return result


def update_job_progress(job_id: str, current: int, total: int) -> None:
    """Update progress counters. Called after each business is scraped."""
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, progress_current = ?, progress_total = ?, updated_at = ?
            WHERE id = ?
        """,
            (STATUS_RUNNING, current, total, _now(), job_id),
        )


def complete_job(job_id: str, results: dict | list) -> None:
    """Mark job done and store results JSON."""
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, results = ?, updated_at = ?
            WHERE id = ?
        """,
            (STATUS_DONE, json.dumps(results), _now(), job_id),
        )


def store_priced_services(job_id: str, services: list[dict]) -> None:
    """Insert priced services for a job. Deduplicates on (job_id, business_name, service_name)."""
    if not services:
        return
    now = _now()
    with _get_conn() as conn:
        for svc in services:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO priced_services
                      (job_id, business_name, service_name, price_value, price_unit, source, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        svc.get("business_name", ""),
                        svc.get("service_name", ""),
                        svc.get("price_value", 0.0),
                        svc.get("price_unit", ""),
                        svc.get("source", "checkatrade"),
                        now,
                    ),
                )
            except Exception:
                pass  # skip individual insert failures, don't abort the batch


def get_priced_services(job_id: str) -> list[dict]:
    """Return all priced services for a job as a list of dicts."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM priced_services WHERE job_id = ? ORDER BY service_name, price_value",
            (job_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def fail_job(job_id: str, error_message: str, timed_out: bool = False) -> None:
    """Mark job as error or timeout with a human-readable message."""
    status = STATUS_TIMEOUT if timed_out else STATUS_ERROR
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, error_message = ?, updated_at = ?
            WHERE id = ?
        """,
            (status, error_message, _now(), job_id),
        )
