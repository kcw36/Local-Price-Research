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
    """Create tables if they don't exist. Safe to call multiple times."""
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
