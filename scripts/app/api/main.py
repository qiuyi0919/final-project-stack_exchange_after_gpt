from __future__ import annotations

from typing import Any, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

try:
    from .db import get_connection
except ImportError:  # Allows `uvicorn main:app` from inside api/.
    from db import get_connection


app = FastAPI(
    title="Stack Overflow CQI Serving API",
    description="API-backed serving layer for monthly CQI trends and RDD results.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SCRIPTS_ROOT.parent

for route, directory in [
    ("/frontend", SCRIPTS_ROOT / "frontend"),
    ("/outputs", PROJECT_ROOT / "outputs"),
    ("/ml_outputs", PROJECT_ROOT / "outputs" / "ml"),
    ("/dask_eda_outputs", PROJECT_ROOT / "outputs" / "dask_eda"),
    ("/outputs_from_emr", PROJECT_ROOT / "outputs" / "emr"),
]:
    if directory.exists():
        app.mount(route, StaticFiles(directory=directory), name=route.strip("/"))


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/frontend/index.html")


def rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "cqi-api"}


@app.get("/metadata")
def metadata() -> dict[str, Any]:
    with get_connection() as conn:
        tiers = [
            row["experience_tier"]
            for row in conn.execute(
                "SELECT DISTINCT experience_tier FROM monthly_cqi ORDER BY experience_tier"
            ).fetchall()
        ]
        categories = [
            row["tag_category"]
            for row in conn.execute(
                "SELECT DISTINCT tag_category FROM monthly_cqi ORDER BY tag_category"
            ).fetchall()
        ]
        monthly_summary = conn.execute(
            """
            SELECT
              COUNT(*) AS monthly_rows,
              MIN(year_month) AS min_month,
              MAX(year_month) AS max_month,
              SUM(n_posts) AS total_posts
            FROM monthly_cqi
            """
        ).fetchone()
        rdd_summary = conn.execute("SELECT COUNT(*) AS rdd_rows FROM rdd_results").fetchone()

    return {
        "experience_tiers": tiers,
        "tag_categories": categories,
        "date_range": {
            "min_month": monthly_summary["min_month"],
            "max_month": monthly_summary["max_month"],
        },
        "row_counts": {
            "monthly_cqi": monthly_summary["monthly_rows"],
            "rdd_results": rdd_summary["rdd_rows"],
        },
        "total_posts": monthly_summary["total_posts"],
    }


@app.get("/trend")
def trend(
    experience_tier: str = Query(..., min_length=1),
    tag_category: str = Query(..., min_length=1),
) -> dict[str, Any]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              year_month,
              experience_tier,
              tag_category,
              mean_CQI,
              n_posts,
              mean_score,
              mean_answer_count,
              month
            FROM monthly_cqi
            WHERE experience_tier = ? AND tag_category = ?
            ORDER BY year_month
            """,
            (experience_tier, tag_category),
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No monthly CQI rows for selected filters")

    return {
        "experience_tier": experience_tier,
        "tag_category": tag_category,
        "rows": rows_to_dicts(rows),
    }


@app.get("/rdd-results")
def rdd_results(
    experience_tier: Optional[str] = None,
    tag_category: Optional[str] = None,
) -> dict[str, Any]:
    params: list[str] = []
    where = ""
    if experience_tier and tag_category:
        where = "WHERE experience_tier = ? AND tag_category = ?"
        params = [experience_tier, tag_category]
    elif experience_tier or tag_category:
        raise HTTPException(
            status_code=400,
            detail="Provide both experience_tier and tag_category, or neither.",
        )

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
              experience_tier,
              tag_category,
              beta_post,
              pvalue,
              ci_low,
              ci_high,
              n_months,
              pre_months,
              post_months,
              n_posts
            FROM rdd_results
            {where}
            ORDER BY experience_tier, tag_category
            """,
            params,
        ).fetchall()

    if experience_tier and tag_category:
        if not rows:
            raise HTTPException(status_code=404, detail="No RDD result for selected filters")
        return {"result": dict(rows[0])}

    return {"rows": rows_to_dicts(rows)}


@app.get("/batch-status")
def batch_status() -> dict[str, Any]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT batch_id, s3_path, size_bytes, uploaded_at, processed_at, status
            FROM batch_manifest
            ORDER BY uploaded_at DESC, batch_id
            """
        ).fetchall()
        summary = conn.execute(
            """
            SELECT status, COUNT(*) AS n_batches, COALESCE(SUM(size_bytes), 0) AS size_bytes
            FROM batch_manifest
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()

    return {
        "summary": rows_to_dicts(summary),
        "batches": rows_to_dicts(rows),
    }
