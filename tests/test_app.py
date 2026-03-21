"""
Tests for app.py — FastAPI routes.

Uses httpx AsyncClient for endpoint testing.
Background jobs are mocked — no real scraping.
"""

import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

import database


@pytest.fixture(autouse=True)
def tmp_db(tmp_path, monkeypatch):
    """Use a temp DB for every test."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database.settings, "database_path", db_path)
    database.init_db()


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    """Async HTTP client pointed at the FastAPI app."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(database.settings, "database_path", db_path)
    database.init_db()

    from app import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_form_returns_200(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "Local Pricing Research" in resp.text or "search" in resp.text.lower()


# ---------------------------------------------------------------------------
# POST /search — validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_search_empty_area_returns_422(client):
    resp = await client.post("/search", data={"area": "", "trade_type": "plumbers"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_start_search_special_chars_returns_422(client):
    resp = await client.post("/search", data={"area": "Sol!hull", "trade_type": "plumbers"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_start_search_valid_returns_job_id(client, monkeypatch):
    """Valid POST should start a job and return job_id."""
    # Mock the background task so it doesn't actually run
    async def mock_run(job_id, area, trade_type):
        database.complete_job(job_id, [])

    import app as app_module
    monkeypatch.setattr(app_module, "run_scrape_job", mock_run)

    resp = await client.post("/search", data={"area": "Solihull", "trade_type": "plumbers"})
    assert resp.status_code == 200
    data = resp.json()
    assert "job_id" in data
    assert len(data["job_id"]) == 36  # UUID


# ---------------------------------------------------------------------------
# GET /status/{job_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_unknown_job_returns_404(client):
    resp = await client.get("/status/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_status_pending_job(client):
    job_id = database.create_job("Solihull", "plumbers")
    resp = await client.get(f"/status/{job_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "pending"
    assert data["job_id"] == job_id


@pytest.mark.asyncio
async def test_status_running_job_shows_progress(client):
    job_id = database.create_job("Solihull", "plumbers")
    database.update_job_progress(job_id, 8, 20)
    resp = await client.get(f"/status/{job_id}")
    data = resp.json()
    assert data["status"] == "running"
    assert data["progress_current"] == 8
    assert data["progress_total"] == 20


@pytest.mark.asyncio
async def test_status_done_job_includes_results(client):
    job_id = database.create_job("Solihull", "plumbers")
    database.complete_job(job_id, {"businesses": [], "summary": {"sample_size": 0}})
    resp = await client.get(f"/status/{job_id}")
    data = resp.json()
    assert data["status"] == "done"
    assert data["results"] is not None


@pytest.mark.asyncio
async def test_status_error_job_includes_message(client):
    job_id = database.create_job("Solihull", "plumbers")
    database.fail_job(job_id, "Something broke")
    resp = await client.get(f"/status/{job_id}")
    data = resp.json()
    assert data["status"] == "error"
    assert data["error_message"] == "Something broke"


# ---------------------------------------------------------------------------
# GET /results/{job_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_results_unknown_job_returns_404(client):
    resp = await client.get("/results/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_results_running_job_shows_loading_page(client):
    job_id = database.create_job("Solihull", "plumbers")
    database.update_job_progress(job_id, 3, 10)
    resp = await client.get(f"/results/{job_id}")
    assert resp.status_code == 200
    # Loading page should poll, not show final results
    assert "loading" in resp.text.lower() or "scanning" in resp.text.lower()


@pytest.mark.asyncio
async def test_results_done_job_shows_results_page(client):
    job_id = database.create_job("Solihull", "plumbers")
    results = {
        "businesses": [
            {
                "name": "Test Plumber Ltd",
                "website": "https://testplumber.example.com",
                "phone": "0121 000 0000",
                "prices": [{"price": 75.0, "service": "hourly rate", "unit": "per hour", "confidence": "Med", "raw_text": "£75 per hour"}],
                "extraction_method": "regex",
                "source_url": "https://testplumber.example.com",
                "area": "Solihull",
                "trade_type": "plumbers",
            }
        ],
        "summary": {
            "summary_text": "Solihull plumbers charge around £75/hr.",
            "sample_size": 1,
            "price_range": {"min": 75.0, "max": 75.0, "median": 75.0},
            "low_sample_warning": True,
        },
    }
    database.complete_job(job_id, results)
    resp = await client.get(f"/results/{job_id}")
    assert resp.status_code == 200
    assert "Test Plumber Ltd" in resp.text
    assert "75" in resp.text


@pytest.mark.asyncio
async def test_results_error_job_shows_error_message(client):
    job_id = database.create_job("Solihull", "plumbers")
    database.fail_job(job_id, "Scan timed out — try a smaller area.")
    resp = await client.get(f"/results/{job_id}")
    assert resp.status_code == 200
    assert "timed out" in resp.text.lower() or "error" in resp.text.lower()


# ---------------------------------------------------------------------------
# Two separate jobs don't interfere
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_jobs_have_independent_status(client):
    id_a = database.create_job("Solihull", "plumbers")
    id_b = database.create_job("Birmingham", "electricians")
    database.complete_job(id_a, [])
    database.fail_job(id_b, "Error")

    resp_a = await client.get(f"/status/{id_a}")
    resp_b = await client.get(f"/status/{id_b}")

    assert resp_a.json()["status"] == "done"
    assert resp_b.json()["status"] == "error"
