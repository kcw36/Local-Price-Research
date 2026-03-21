"""
Tests for database.py — SQLite job CRUD.

Uses a temporary in-memory DB for each test via monkeypatch.
"""

import pytest

import database


@pytest.fixture(autouse=True)
def tmp_db(tmp_path, monkeypatch):
    """Redirect all DB operations to a temporary file, not the real jobs.db."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database.settings, "database_path", db_path)
    database.init_db()
    yield db_path


# ---------------------------------------------------------------------------
# create_job / get_job
# ---------------------------------------------------------------------------


def test_create_job_returns_uuid_string():
    job_id = database.create_job("Solihull", "plumbers")
    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID format


def test_get_job_returns_dict():
    job_id = database.create_job("Solihull", "plumbers")
    job = database.get_job(job_id)
    assert job is not None
    assert job["id"] == job_id
    assert job["area"] == "Solihull"
    assert job["trade_type"] == "plumbers"
    assert job["status"] == database.STATUS_PENDING


def test_get_job_returns_none_for_unknown_id():
    result = database.get_job("00000000-0000-0000-0000-000000000000")
    assert result is None


def test_new_job_has_zero_progress():
    job_id = database.create_job("Birmingham", "electricians")
    job = database.get_job(job_id)
    assert job["progress_current"] == 0
    assert job["progress_total"] == 0


# ---------------------------------------------------------------------------
# update_job_progress
# ---------------------------------------------------------------------------


def test_update_progress_changes_status_to_running():
    job_id = database.create_job("Solihull", "plumbers")
    database.update_job_progress(job_id, 5, 20)
    job = database.get_job(job_id)
    assert job["status"] == database.STATUS_RUNNING
    assert job["progress_current"] == 5
    assert job["progress_total"] == 20


def test_update_progress_increments():
    job_id = database.create_job("Solihull", "plumbers")
    database.update_job_progress(job_id, 1, 10)
    database.update_job_progress(job_id, 5, 10)
    database.update_job_progress(job_id, 10, 10)
    job = database.get_job(job_id)
    assert job["progress_current"] == 10
    assert job["progress_total"] == 10


# ---------------------------------------------------------------------------
# complete_job
# ---------------------------------------------------------------------------


def test_complete_job_sets_done_status():
    job_id = database.create_job("Solihull", "plumbers")
    results = [{"name": "Test Plumber", "prices": [{"price": 75.0}]}]
    database.complete_job(job_id, results)
    job = database.get_job(job_id)
    assert job["status"] == database.STATUS_DONE


def test_complete_job_stores_results_as_list():
    job_id = database.create_job("Solihull", "plumbers")
    results = [{"name": "Test Plumber", "prices": [{"price": 75.0, "service": "hourly"}]}]
    database.complete_job(job_id, results)
    job = database.get_job(job_id)
    assert isinstance(job["results"], list)
    assert job["results"][0]["name"] == "Test Plumber"


def test_complete_job_with_empty_results():
    job_id = database.create_job("Solihull", "plumbers")
    database.complete_job(job_id, [])
    job = database.get_job(job_id)
    assert job["status"] == database.STATUS_DONE
    assert job["results"] == []


# ---------------------------------------------------------------------------
# fail_job
# ---------------------------------------------------------------------------


def test_fail_job_sets_error_status():
    job_id = database.create_job("Solihull", "plumbers")
    database.fail_job(job_id, "Something went wrong")
    job = database.get_job(job_id)
    assert job["status"] == database.STATUS_ERROR
    assert job["error_message"] == "Something went wrong"


def test_fail_job_with_timeout_flag():
    job_id = database.create_job("Solihull", "plumbers")
    database.fail_job(job_id, "Timed out", timed_out=True)
    job = database.get_job(job_id)
    assert job["status"] == database.STATUS_TIMEOUT


# ---------------------------------------------------------------------------
# Multiple jobs don't interfere
# ---------------------------------------------------------------------------


def test_two_jobs_are_independent():
    id_a = database.create_job("Solihull", "plumbers")
    id_b = database.create_job("Birmingham", "electricians")

    database.complete_job(id_a, [{"name": "Plumber A"}])
    database.fail_job(id_b, "Error B")

    job_a = database.get_job(id_a)
    job_b = database.get_job(id_b)

    assert job_a["status"] == database.STATUS_DONE
    assert job_b["status"] == database.STATUS_ERROR
