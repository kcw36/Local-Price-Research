"""
app.py — FastAPI web application.

Routes:
  GET  /                  — Search form
  POST /search            — Start scrape job; returns {job_id}
  GET  /status/{job_id}   — Job status, progress, results (JSON)
  GET  /results/{job_id}  — Results page (HTML, only when done)

Async background task pattern:
  POST /search
    → validate input
    → create SQLite job
    → asyncio.create_task(run_scrape_job(job_id, area, trade_type))
    → return {job_id} immediately

  GET /status/{job_id}
    → read SQLite
    → return {status, progress_current, progress_total, results, summary}

Frontend polls /status every 3 seconds, shows progress bar,
then redirects to /results when status == "done".
"""

import asyncio
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from config import settings
from database import (
    complete_job,
    create_job,
    fail_job,
    get_job,
    init_db,
    update_job_progress,
)
from scraper import scrape_all
from summary import generate_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("Database initialised at %s", settings.database_path)
    yield


app = FastAPI(title="Local Pricing Research", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

_VALID_AREA = re.compile(r"^[a-zA-Z0-9\s,\-\.]+$")
_VALID_TRADE = re.compile(r"^[a-zA-Z\s\-]+$")
_MAX_LEN = 100


def _validate_input(area: str, trade_type: str) -> tuple[bool, str]:
    """Return (valid, error_message)."""
    area = area.strip()
    trade_type = trade_type.strip()

    if not area:
        return False, "Area is required."
    if not trade_type:
        return False, "Trade type is required."
    if len(area) > _MAX_LEN:
        return False, f"Area must be {_MAX_LEN} characters or fewer."
    if len(trade_type) > _MAX_LEN:
        return False, f"Trade type must be {_MAX_LEN} characters or fewer."
    if not _VALID_AREA.match(area):
        return False, "Area contains invalid characters. Use letters, numbers, spaces, commas, hyphens, or dots."
    if not _VALID_TRADE.match(trade_type):
        return False, "Trade type contains invalid characters. Use letters, spaces, or hyphens."

    return True, ""


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------


async def run_scrape_job(job_id: str, area: str, trade_type: str) -> None:
    """
    Background coroutine. Wraps scrape_all() in asyncio.wait_for() for timeout.
    Updates SQLite throughout; never raises (all errors are captured to DB).
    """

    def _progress(current: int, total: int) -> None:
        update_job_progress(job_id, current, total)

    try:
        businesses = await asyncio.wait_for(
            scrape_all(area, trade_type, progress_cb=_progress),
            timeout=settings.job_timeout,
        )

        # Attach area/trade_type to each business for context in summary
        for b in businesses:
            b["area"] = area
            b["trade_type"] = trade_type

        summary = generate_summary(businesses)

        complete_job(
            job_id,
            results={"businesses": businesses, "summary": summary},
        )
        logger.info(
            "Job %s complete — %d businesses, %d prices",
            job_id,
            len(businesses),
            summary["sample_size"],
        )

    except asyncio.TimeoutError:
        msg = "Scan timed out — try a smaller area or fewer businesses."
        fail_job(job_id, msg, timed_out=True)
        logger.warning("Job %s timed out after %ds", job_id, settings.job_timeout)

    except Exception as e:
        msg = f"Unexpected error: {type(e).__name__}: {e}"
        fail_job(job_id, msg)
        logger.exception("Job %s failed: %s", job_id, e)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def search_form(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "default_area": settings.area,
            "default_trade": settings.trade_type,
        },
    )


@app.post("/search")
async def start_search(
    area: str = Form(...),
    trade_type: str = Form(...),
):
    valid, error = _validate_input(area, trade_type)
    if not valid:
        raise HTTPException(status_code=422, detail=error)

    job_id = create_job(area.strip(), trade_type.strip())
    asyncio.create_task(run_scrape_job(job_id, area.strip(), trade_type.strip()))

    return JSONResponse({"job_id": job_id})


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")

    return JSONResponse(
        {
            "job_id": job_id,
            "status": job["status"],
            "progress_current": job["progress_current"],
            "progress_total": job["progress_total"],
            "area": job["area"],
            "trade_type": job["trade_type"],
            "results": job.get("results"),
            "error_message": job.get("error_message"),
        }
    )


@app.get("/results/{job_id}", response_class=HTMLResponse)
async def results_page(request: Request, job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")

    if job["status"] not in ("done", "error", "timeout"):
        # Still running — redirect back to the polling page
        return templates.TemplateResponse(
            request,
            "loading.html",
            {
                "job_id": job_id,
                "area": job["area"],
                "trade_type": job["trade_type"],
            },
        )

    results = job.get("results") or {}
    businesses = results.get("businesses", [])
    summary = results.get("summary", {})

    return templates.TemplateResponse(
        request,
        "results.html",
        {
            "job": job,
            "businesses": businesses,
            "summary": summary,
            "area": job["area"],
            "trade_type": job["trade_type"],
        },
    )
